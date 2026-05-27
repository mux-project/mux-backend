"""Database writer for alert history — the consistency layer.

Handles all alert_history INSERT/UPDATE operations with:
  - Coordinated DB + Redis state management
  - IntegrityError recovery (rebuild Redis from DB)
  - Optimistic locking on resolve to prevent stale-wins-latest races
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.alerts import metrics
from app.alerts.cache import CachedRule
from app.alerts.notifier import dispatch_notifications
from app.alerts.state import StateManager
from app.core.logging import logger
from app.database.session import async_session_factory
from app.models.alert_breach import AlertBreach
from app.models.alert_history import AlertHistory

# --- Notification task tracking (N21 — graceful drain on shutdown) ---

_notification_tasks: set[asyncio.Task] = set()


def _spawn_notification(coro) -> asyncio.Task:
    """Fire-and-forget a notification coroutine, tracking it for drain."""
    task = asyncio.ensure_future(coro)
    _notification_tasks.add(task)
    task.add_done_callback(_notification_tasks.discard)
    return task


async def drain_notification_tasks(timeout: float = 10) -> None:
    """Await all in-flight notification tasks during shutdown."""
    if not _notification_tasks:
        return
    tasks = list(_notification_tasks)
    _notification_tasks.clear()
    done, pending = await asyncio.wait(tasks, timeout=timeout)
    for t in pending:
        t.cancel()


@asynccontextmanager
async def _session_scope(db_session: AsyncSession | None):
    """Yield the caller-provided session, or create + close a new one (N4)."""
    if db_session is not None:
        yield db_session
    else:
        async with async_session_factory() as s:
            yield s


async def fire_alert(
    state: StateManager,
    rule: CachedRule,
    node_id: uuid.UUID,
    value: float,
    tenant_id: str = "",
) -> uuid.UUID | None:
    """Insert a new firing alert into the database.

    Returns the alert_history UUID if a new alert was fired,
    or None if the alert was already active (dedup).

    Safety interlocks:
      1. Checks active + cooldown in Redis before DB write
      2. Handles IntegrityError (unique index violation) by
         rebuilding Redis state from the existing DB row
      3. Always keeps DB as source of truth
    """
    # 1. Pre-check: is this (rule, node) already firing or in cooldown?
    #    (The caller should have checked this, but double-check for safety)
    if await state.get_active_alert(rule.id, node_id):
        logger.debug("fire_skipped_active_exists", rule_id=str(rule.id), node_id=str(node_id))
        return None
    if await state.is_in_cooldown(rule.id, node_id):
        logger.debug("fire_skipped_cooldown", rule_id=str(rule.id), node_id=str(node_id))
        return None

    # 2. Use Redis clock for both DB and Redis state (N19 — prevent
    #    silent resolve failures under clock drift between app and Redis).
    redis_now = await state.redis_time()
    triggered_at = datetime.fromtimestamp(redis_now, tz=timezone.utc)
    alert = AlertHistory(
        rule_id=rule.id,
        node_id=node_id,
        tenant_id=tenant_id,
        status="firing",
        metric_value=value,
        message=f"Alert: {rule.name} — {rule.metric_field} {rule.operator} {rule.threshold} (value: {value})",
        triggered_at=triggered_at,
        rule_version=rule.version,
        last_notified_at=triggered_at,
    )

    # 3. Insert into DB (with IntegrityError recovery)
    async with async_session_factory() as session:
        try:
            session.add(alert)
            await session.commit()
            await session.refresh(alert)
            logger.info(
                "alert_fired",
                rule_id=str(rule.id),
                rule_name=rule.name,
                node_id=str(node_id),
                value=value,
                threshold=rule.threshold,
                alert_history_id=str(alert.id),
            )
        except IntegrityError:
            await session.rollback()
            metrics.alert_db_errors.labels(operation="integrity").inc()
            logger.warning(
                "fire_integrity_error_recovering",
                rule_id=str(rule.id),
                node_id=str(node_id),
                tenant_id=tenant_id,
            )
            alert_history_id = await _rebuild_active_from_db(state, rule, node_id, db_session=session)
            if alert_history_id:
                logger.info(
                    "fire_conflict_resolved_reusing_existing",
                    rule_id=str(rule.id),
                    node_id=str(node_id),
                    tenant_id=tenant_id,
                    alert_history_id=str(alert_history_id),
                )
            return alert_history_id

        # 4. Set Redis active alert state (using same redis_now as DB)
        await state.set_active_alert(
            rule_id=rule.id,
            node_id=node_id,
            alert_history_id=alert.id,
            rule_version=rule.version,
            value=value,
            fired_at=redis_now,
        )

        # 5. Clean up any legacy no-tenant key that may exist from a
        #    pre-F1 crash recovery (F1 — prevents ghost renotify spam
        #    when a recovered no-tenant active key survives alongside
        #    this new tenant-scoped key).
        legacy_key = f"active:{state._uuid_key(rule.id)}:{state._uuid_key(node_id)}"
        if await state._r.exists(legacy_key):
            await state._r.srem("active_set", legacy_key)
            await state._r.delete(legacy_key)
            logger.info(
                "legacy_no_tenant_active_cleaned",
                rule_id=str(rule.id),
                node_id=str(node_id),
            )

        # 7. Dispatch notification in background task (H4 — avoid blocking the hot path)
        _spawn_notification(
            dispatch_notifications(
                rule=rule, node_id=node_id, value=value, alert_history_id=alert.id,
                tenant_id=tenant_id,
            )
        )

        # 8. Metrics
        metrics.alert_fired.labels(rule_name=rule.name, tenant=tenant_id).inc()

        return alert.id

    return None


async def resolve_alert(
    state: StateManager,
    rule: CachedRule,
    node_id: uuid.UUID,
    active_alert: dict,
    tenant_id: str = "",
) -> bool:
    """Resolve an actively firing alert.

    Uses optimistic locking to prevent the stale-wins-latest race:
      UPDATE alert_history SET status='resolved'
      WHERE id = :id AND status = 'firing' AND triggered_at = :triggered_at

    Returns True if the alert was resolved, False if it was
    already resolved (or superseded by a newer firing).
    """
    alert_history_id = active_alert["alert_history_id"]
    triggered_at = datetime.fromtimestamp(active_alert["fired_at"], tz=timezone.utc)

    async with async_session_factory() as session:
        # 1. Optimistic-locked update
        redis_now = await state.redis_time()
        resolved_at = datetime.fromtimestamp(redis_now, tz=timezone.utc)
        result = await session.execute(
            update(AlertHistory)
            .where(
                AlertHistory.id == alert_history_id,
                AlertHistory.status == "firing",
                AlertHistory.triggered_at == triggered_at,
            )
            .values(
                status="resolved",
                resolved_at=resolved_at,
            )
        )

        if result.rowcount == 0:
            # Alert was already resolved by another worker, or was
            # superseded by a new firing. Clean up Redis state.
            logger.warning(
                "resolve_noop_already_resolved_or_superseded",
                alert_history_id=str(alert_history_id),
                rule_id=str(rule.id),
                node_id=str(node_id),
            )
            # Clean up any stale Redis keys
            await state.delete_active_alert(rule.id, node_id)
            await state.delete_breach_window(rule.id, node_id)
            return False

        await session.commit()

        # 2. Clean up Redis state
        await state.delete_active_alert(rule.id, node_id)
        await state.delete_breach_window(rule.id, node_id)

        # 3. Metrics
        metrics.alert_resolved.labels(rule_name=rule.name, tenant=tenant_id).inc()

        # 4. Dispatch resolve notification in background task (H4)
        _spawn_notification(
            dispatch_notifications(
                rule=rule,
                node_id=node_id,
                value=active_alert.get("value", 0.0),
                alert_history_id=alert_history_id,
                is_resolve=True,
                tenant_id=tenant_id,
            )
        )

        # 5. Set cooldown
        await state.set_cooldown(rule.id, node_id, rule.cooldown_seconds)

        duration_seconds = redis_now - active_alert["fired_at"]
        logger.info(
            "alert_resolved",
            rule_id=str(rule.id),
            rule_name=rule.name,
            node_id=str(node_id),
            alert_history_id=str(alert_history_id),
            duration_seconds=duration_seconds,
        )
        return True


async def _rebuild_active_from_db(
    state: StateManager,
    rule: CachedRule,
    node_id: uuid.UUID,
    db_session: AsyncSession | None = None,
) -> uuid.UUID | None:
    """Rebuild Redis active alert state from the database.

    Called when an IntegrityError indicates that a firing alert
    already exists for this (rule, node, tenant) — some other worker
    beat us to it. We restore Redis state from the DB row.
    Must scope by tenant_id to avoid cross-tenant recovery (F1).

    Returns the alert_history_id of the existing firing alert.
    """
    async with _session_scope(db_session) as session:
        result = await session.execute(
            select(AlertHistory)
            .where(
                AlertHistory.rule_id == rule.id,
                AlertHistory.node_id == node_id,
                AlertHistory.tenant_id == state._tenant,
                AlertHistory.status == "firing",
            )
            .order_by(AlertHistory.triggered_at.desc())
            .limit(1)
        )
        existing = result.scalar_one_or_none()

        if existing is None:
            # No existing firing alert — this shouldn't happen with
            # IntegrityError, but handle it gracefully
            logger.error(
                "rebuild_active_failed_no_firing_row",
                rule_id=str(rule.id),
                node_id=str(node_id),
            )
            return None

        redis_now = await state.redis_time()
        notified_ts = existing.last_notified_at.timestamp() if existing.last_notified_at else existing.triggered_at.timestamp()

        await state.set_active_alert(
            rule_id=rule.id,
            node_id=node_id,
            alert_history_id=existing.id,
            rule_version=existing.rule_version or rule.version,
            value=existing.metric_value,
            fired_at=existing.triggered_at.timestamp(),
            last_notified_at=notified_ts,
            notified_count=existing.notified_count or 0,
        )

        logger.info(
            "active_alert_rebuilt_from_db",
            rule_id=str(rule.id),
            node_id=str(node_id),
            alert_history_id=str(existing.id),
        )
        return existing.id


async def persist_breach_window(
    rule_id: uuid.UUID,
    node_id: uuid.UUID,
    started_at: float,
    tenant_id: str = "",
    db_session: AsyncSession | None = None,
) -> None:
    """Persist a breach window to the database for crash recovery.

    Uses PostgreSQL ON CONFLICT (rule_id, node_id, tenant_id) DO NOTHING
    to handle concurrent upserts safely (H2).
    Accepts an optional pre-existing session for batch reuse (N4).
    """
    async with _session_scope(db_session) as session:
        try:
            await session.execute(
                AlertBreach.__table__.insert().on_conflict_do_nothing(
                ).values(
                    rule_id=rule_id,
                    node_id=node_id,
                    tenant_id=tenant_id,
                    started_at=datetime.fromtimestamp(started_at, tz=timezone.utc),
                )
            )
            await session.commit()
        except Exception:
            await session.rollback()
            logger.exception("persist_breach_window_failed", rule_id=str(rule_id), node_id=str(node_id), tenant_id=tenant_id)


async def delete_breach_window_db(
    rule_id: uuid.UUID,
    node_id: uuid.UUID,
    tenant_id: str = "",
    db_session: AsyncSession | None = None,
) -> None:
    """Remove a breach window from the database (alert fired or cleared).

    Scoped by tenant_id to avoid cross-tenant deletion (H2).
    Accepts an optional pre-existing session for batch reuse (N4).
    """
    async with _session_scope(db_session) as session:
        try:
            await session.execute(
                AlertBreach.__table__.delete().where(
                    AlertBreach.rule_id == rule_id,
                    AlertBreach.node_id == node_id,
                    AlertBreach.tenant_id == tenant_id,
                )
            )
            await session.commit()
        except Exception:
            await session.rollback()
            logger.exception("delete_breach_window_failed", rule_id=str(rule_id), node_id=str(node_id), tenant_id=tenant_id)
