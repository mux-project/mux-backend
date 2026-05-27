"""Re-notification and ghost alert watchdog.

Periodic loop (every 60s) that:
  1. Scans all active alerts in Redis
  2. Verifies each against DB (ghost detection)
  3. Checks re-notification interval has elapsed
  4. Checks value change threshold (>= 10% change)
  5. Handles escalation levels
  6. Cleans up ghost alerts (active Redis, resolved DB)

Uses redis.time() for all timing decisions.
"""

import asyncio
import uuid
from datetime import datetime, timezone

from sqlalchemy import select

from app.alerts import metrics
from app.alerts.cache import CachedRule, rule_cache
from app.alerts.notifier import dispatch_notifications
from app.alerts.state import StateManager
from app.core.logging import logger
from app.database.session import async_session_factory
from app.models.alert_history import AlertHistory

# Minimum percentage change to trigger a re-notification
NOTIFY_CHANGE_THRESHOLD_PCT = 10.0

# Maximum number of re-notifications per alert cycle (rate limiting)
MAX_RENOTIFICATIONS = 10


def _escalation_level(elapsed_minutes: float) -> int:
    """Determine escalation level based on time since alert fired.

    Level 0: 0-15 min  — standard notification
    Level 1: 15-30 min — re-notify
    Level 2: 30-60 min — escalate (email + slack)
    Level 3: 60+ min   — critical (all channels)
    """
    if elapsed_minutes < 15:
        return 0
    if elapsed_minutes < 30:
        return 1
    if elapsed_minutes < 60:
        return 2
    return 3


async def renotify_loop(
    state: StateManager,
    interval: int = 60,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Run the re-notification + watchdog loop periodically.

    This runs as a background task in the worker.
    """
    logger.info("renotify_loop_starting", interval_seconds=interval)

    while True:
        try:
            if stop_event and stop_event.is_set():
                break

            await asyncio.sleep(interval)
            await _run_renotify_cycle(state)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("renotify_loop_error", error=str(exc))

    logger.info("renotify_loop_stopped")


async def _scan_all_active_alerts(redis) -> list[tuple[uuid.UUID, uuid.UUID, dict, str]]:
    """Scan ALL active_set variants — no-tenant and per-tenant (F2).

    Returns (rule_id, node_id, alert_state, tenant_id) tuples for
    every active alert across all tenants.
    """
    results = []
    seen = set()

    async for set_key in redis.scan_iter(match="active_set*"):
        member_keys = await redis.smembers(set_key)
        tenant = set_key[len("active_set:"):] if ":" in set_key else ""

        for key in member_keys:
            if key in seen:
                continue
            seen.add(key)
            raw = await redis.hgetall(key)
            if not raw:
                continue
            parts = key.split(":")
            results.append((
                uuid.UUID(hex=parts[-2]),
                uuid.UUID(hex=parts[-1]),
                {
                    "alert_history_id": uuid.UUID(raw["alert_history_id"]),
                    "rule_version": int(raw["rule_version"]),
                    "value": float(raw["value"]),
                    "fired_at": float(raw["fired_at"]),
                    "last_notified_at": float(raw["last_notified_at"]),
                    "notified_count": int(raw.get("notified_count", 0)),
                },
                tenant,
            ))
    return results


async def _run_renotify_cycle(state: StateManager) -> None:
    """Single re-notify cycle — scan all tenants, verify, dispatch, clean up (F2)."""
    redis_now = await state.redis_time()
    active_alerts = await _scan_all_active_alerts(state._r)

    metrics.alert_active_count.set(len(active_alerts))

    if not active_alerts:
        return

    # Batch-check all alerts' DB status in a single query (H1 — kills N+1)
    firing_states = await _batch_check_firing([a[2]["alert_history_id"] for _, _, a, _ in active_alerts])

    for rule_id, node_id, alert_state, tenant_id in active_alerts:
        # Use tenant-scoped state for Redis operations (F2)
        ts = StateManager(state._r, tenant=tenant_id)
        alert_history_id = alert_state["alert_history_id"]
        fired_at = alert_state["fired_at"]
        last_notified_at = alert_state["last_notified_at"]
        current_value = alert_state["value"]
        notified_count = alert_state.get("notified_count", 0)

        # 1. Check DB: is this alert still firing? (batched)
        still_firing = firing_states.get(alert_history_id, False)
        if not still_firing:
            # Ghost alert — clean up Redis key
            await ts.delete_active_alert(rule_id, node_id)
            await ts.delete_breach_window(rule_id, node_id)
            logger.warning(
                "ghost_alert_cleaned",
                rule_id=str(rule_id),
                node_id=str(node_id),
                alert_history_id=str(alert_history_id),
            )
            continue

        # 2. Check if the rule still exists and is active
        rule = rule_cache.get_by_id(rule_id)
        if not rule or not rule.is_active:
            # Rule was deleted or disabled — force resolve
            await ts.delete_active_alert(rule_id, node_id)
            async with async_session_factory() as session:
                await session.execute(
                    AlertHistory.__table__.update()
                    .where(AlertHistory.id == alert_history_id)
                    .values(status="resolved", resolved_at=datetime.now(timezone.utc))
                )
                await session.commit()
            logger.info(
                "alert_force_resolved_rule_disabled",
                rule_id=str(rule_id),
                alert_history_id=str(alert_history_id),
            )
            continue

        # 3. Check elapsed time since renotify
        elapsed_since_notify = redis_now - last_notified_at
        if elapsed_since_notify < rule.renotify_interval_seconds:
            continue  # Not time yet

        # 4. Check value change threshold
        last_value = alert_state.get("value", current_value)
        change_pct = abs(current_value - last_value) / max(last_value, 0.01) * 100
        if change_pct < NOTIFY_CHANGE_THRESHOLD_PCT:
            continue  # Not enough change — skip

        # 5. Rate limiting: cap re-notifications per alert cycle
        if notified_count >= MAX_RENOTIFICATIONS:
            logger.debug(
                "renotify_rate_limited",
                rule_id=str(rule_id),
                node_id=str(node_id),
                notified_count=notified_count,
                max=MAX_RENOTIFICATIONS,
            )
            continue

        # 6. Determine escalation level
        elapsed_minutes = (redis_now - fired_at) / 60.0
        escalation = _escalation_level(elapsed_minutes)

        # 7. Dispatch re-notification
        notified = await dispatch_notifications(
            rule=rule,
            node_id=node_id,
            value=current_value,
            alert_history_id=alert_history_id,
            is_renotify=True,
            escalation_level=escalation,
        )

        # 8. Update last_notified_at in Redis AND DB (also increments notified_count)
        await ts.update_last_notified(rule_id, node_id, redis_now)
        await _update_last_notified_db(alert_history_id, redis_now)

        if notified:
            metrics.alert_renotified.labels(
                rule_name=rule.name,
                channel=",".join(notified),
                escalation_level=str(escalation),
            ).inc()
            logger.info(
                "alert_renotified",
                rule_id=str(rule_id),
                rule_name=rule.name,
                node_id=str(node_id),
                escalation_level=escalation,
                channels=notified,
                elapsed_hours=round(elapsed_since_notify / 3600, 2),
            )


async def _batch_check_firing(ids: list[uuid.UUID]) -> dict[uuid.UUID, bool]:
    """Batch-check firing status for multiple alert history IDs (H1).

    Returns {alert_history_id: is_firing} dict with a single DB query.
    """
    if not ids:
        return {}
    async with async_session_factory() as session:
        result = await session.execute(
            select(AlertHistory.id, AlertHistory.status).where(
                AlertHistory.id.in_(ids)
            )
        )
        rows = result.all()
        return {row.id: row.status == "firing" for row in rows}


async def _update_last_notified_db(
    alert_history_id: uuid.UUID, timestamp: float
) -> None:
    """Update last_notified_at in DB for recovery correctness (H2).

    notified_count is NOT written to DB — Redis is the source of truth.
    Only last_notified_at is persisted so that crash recovery can restore
    a meaningful renotify interval check.
    """
    async with async_session_factory() as session:
        await session.execute(
            AlertHistory.__table__.update()
            .where(AlertHistory.id == alert_history_id)
            .values(
                last_notified_at=datetime.fromtimestamp(timestamp, tz=timezone.utc),
            )
        )
        await session.commit()
