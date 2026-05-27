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

    # 3. Clear any stale active_set members from previous lifecycle (H2)
    await state._r.delete(state._active_set_key())

    # 4. Clean up old-format UUID keys (with hyphens) from before F9 fix (H3)
    await _cleanup_legacy_uuids(state)

    # 5. Load all firing alerts from DB
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


UUID_KEY_PATTERN_LEN = 36  # "550e8400-e29b-41d4-a716-446655440000"


async def _cleanup_legacy_uuids(state: StateManager) -> None:
    """Delete Redis keys using the old hyphenated UUID format (before F9 fix).

    After F9, all UUIDs use 32-char hex (no hyphens). Old keys with hyphens
    are orphaned. This deletes them on recovery to free memory.
    """
    r = state._r
    removed = 0
    for prefix in ("active", "breach", "lock:eval", "cooldown"):
        # Match keys with hyphenated UUID segments: XXXX-XXXX-...
        pattern = f"{prefix}:{':*' if state._tenant else ''}*????-*"
        async for key in r.scan_iter(match=pattern):
            # Double-check: is the UUID segment 36 chars with hyphens?
            parts = key.split(":")
            uuid_candidate = parts[-1] if len(parts) >= 2 else ""
            if len(uuid_candidate) == UUID_KEY_PATTERN_LEN and "-" in uuid_candidate:
                await r.delete(key)
                removed += 1

    if removed > 0:
        logger.info("legacy_uuid_keys_cleaned_up", count=removed)
