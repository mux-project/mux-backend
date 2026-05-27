"""Startup recovery — rebuilds Redis state from the database.

On worker boot:
  1. Validates clock drift between Redis and system clock
  2. Loads all active (status='firing') alerts from AlertHistory
  3. Rebuilds Redis active:* keys from DB state
  4. Rules cache is refreshed by the cache manager
"""

import time as time_module
from datetime import timezone

from sqlalchemy import select

from app.alerts.cache import rule_cache
from app.alerts.state import StateManager
from app.core.logging import logger
from app.database.session import async_session_factory
from app.models.alert_breach import AlertBreach
from app.models.alert_history import AlertHistory


async def rebuild_state_from_db(state: StateManager) -> int:
    """Rebuild Redis active alert state from the database.

    Returns the number of active alerts restored.

    This must be called on worker startup BEFORE the consumer
    starts processing messages.
    """
    # 1. Clock drift check
    await _check_clock_drift(state)

    # 2. Ensure rule cache is loaded
    if not rule_cache.is_loaded:
        await rule_cache.refresh()

    # 3. Clear ALL alert state keys — both tenant-scoped and global (N1)
    await _clear_all_state(state)

    # 4. Load all firing alerts from DB
    restored = 0
    async with async_session_factory() as session:
        result = await session.execute(
            select(AlertHistory).where(
                AlertHistory.status == "firing"
            ).order_by(AlertHistory.triggered_at.desc())
        )
        firing_alerts = result.scalars().all()

        for alert in firing_alerts:
            rule = rule_cache.get_by_id(alert.rule_id)
            rule_version = rule.version if rule else (alert.rule_version or 1)
            node_id = alert.node_id

            notified_ts = (
                alert.last_notified_at.timestamp()
                if alert.last_notified_at
                else alert.triggered_at.timestamp()
            )

            await state.set_active_alert(
                rule_id=alert.rule_id,
                node_id=node_id,
                alert_history_id=alert.id,
                rule_version=rule_version,
                value=alert.metric_value,
                fired_at=alert.triggered_at.replace(tzinfo=timezone.utc).timestamp(),
                last_notified_at=notified_ts,
            )

            # If the rule no longer exists or is inactive, force resolve
            if rule is None or not rule.is_active:
                logger.warning(
                    "active_alert_orphaned_rule_disabled",
                    rule_id=str(alert.rule_id),
                    alert_history_id=str(alert.id),
                )
                # Force-resolve: update DB
                alert.status = "resolved"
                # Don't restore in Redis — it gets resolved
                await state.delete_active_alert(alert.rule_id, node_id)

            restored += 1

    # 5. Rebuild breach windows (duration tracking) from DB
    breach_count = await _rebuild_breach_windows(state)

    logger.info(
        "startup_recovery_complete",
        active_alerts_restored=restored,
        breach_windows_restored=breach_count,
        rules_cached=len(rule_cache.all_rules()),
    )
    return restored


async def _check_clock_drift(state: StateManager) -> None:
    """Compare Redis server time with system clock.

    Logs a warning if drift exceeds 1 second.
    This is informational — no corrective action is taken.
    """
    try:
        redis_seconds, redis_micros = await state._r.time()
        redis_time = float(f"{redis_seconds}.{redis_micros:06d}")
        system_time = time_module.time()
        drift = abs(redis_time - system_time)

        if drift > 1.0:
            logger.warning(
                "clock_drift_detected",
                drift_seconds=round(drift, 3),
                redis_time=redis_time,
                system_time=system_time,
            )
        else:
            logger.debug(
                "clock_drift_ok",
                drift_seconds=round(drift, 3),
            )
    except Exception as exc:
        logger.error("clock_drift_check_failed", error=str(exc))


async def _rebuild_breach_windows(state: StateManager) -> int:
    """Restore in-progress breach windows from the database.

    Recreates Redis breach:* keys for duration-based rules whose
    breach window was interrupted by a worker restart.
    """
    restored = 0
    async with async_session_factory() as session:
        result = await session.execute(select(AlertBreach))
        rows = result.scalars().all()

        for row in rows:
            started_ts = row.started_at.replace(tzinfo=timezone.utc).timestamp()
            await state.create_breach_window(row.rule_id, row.node_id, started_ts)
            restored += 1

    if restored > 0:
        logger.info("breach_windows_restored", count=restored)

    return restored


async def _clear_all_state(state: StateManager) -> None:
    """Delete ALL alert engine Redis keys — both tenant-scoped and global (N1).

    Covers every key pattern the engine writes to: active_set, active hashes,
    breach windows, cooldowns, and eval locks. Deletes every variant
    (tenant-prefixed and non-tenant) to guarantee a clean slate before
    rebuilding from the DB.
    """
    r = state._r
    removed = 0
    for pattern in ("active_set*", "active:*", "breach:*", "cooldown:*", "lock:eval:*"):
        async for key in r.scan_iter(match=pattern):
            await r.delete(key)
            removed += 1

    if removed > 0:
        logger.info("alert_state_cleared", keys_removed=removed)
