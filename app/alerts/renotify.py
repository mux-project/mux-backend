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


async def _run_renotify_cycle(state: StateManager) -> None:
    """Single re-notify cycle — scan, verify, dispatch, clean up."""
    redis_now = await state.redis_time()
    active_alerts = await state.scan_active_alerts()

    for rule_id, node_id, alert_state in active_alerts:
        alert_history_id = alert_state["alert_history_id"]
        fired_at = alert_state["fired_at"]
        last_notified_at = alert_state["last_notified_at"]
        current_value = alert_state["value"]
        notified_count = alert_state.get("notified_count", 0)

        # 1. Check DB: is this alert still firing?
        still_firing = await _is_alert_firing(alert_history_id)
        if not still_firing:
            # Ghost alert — clean up Redis key
            await state.delete_active_alert(rule_id, node_id)
            await state.delete_breach_window(rule_id, node_id)
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
            await state.delete_active_alert(rule_id, node_id)
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
        await state.update_last_notified(rule_id, node_id, redis_now)
        await _update_last_notified_db(alert_history_id, redis_now)

        if notified:
            logger.info(
                "alert_renotified",
                rule_id=str(rule_id),
                rule_name=rule.name,
                node_id=str(node_id),
                escalation_level=escalation,
                channels=notified,
                elapsed_hours=round(elapsed_since_notify / 3600, 2),
            )


async def _is_alert_firing(alert_history_id: uuid.UUID) -> bool:
    """Check if an alert_history record still has status='firing'."""
    async with async_session_factory() as session:
        result = await session.execute(
            select(AlertHistory.status).where(AlertHistory.id == alert_history_id)
        )
        row = result.scalar_one_or_none()
        return row == "firing"


async def _update_last_notified_db(
    alert_history_id: uuid.UUID, timestamp: float
) -> None:
    """Update last_notified_at and increment notified_count in the DB."""
    async with async_session_factory() as session:
        await session.execute(
            AlertHistory.__table__.update()
            .where(AlertHistory.id == alert_history_id)
            .values(
                last_notified_at=datetime.fromtimestamp(timestamp, tz=timezone.utc),
                notified_count=AlertHistory.notified_count + 1,
            )
        )
        await session.commit()
