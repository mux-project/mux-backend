"""In-memory rule cache indexed by metric_field for O(1) evaluation lookup.

Refreshes from PostgreSQL every ALERT_ENGINE_CACHE_REFRESH seconds.
Tracks rule version for live-update detection.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.logging import logger
from app.models.alert_rule import AlertRule
from app.database.session import async_session_factory


@dataclass
class CachedRule:
    """Lightweight immutable snapshot of an alert rule for evaluation."""

    id: uuid.UUID
    name: str
    metric_field: str
    operator: str
    threshold: float
    duration_seconds: int
    channels: list
    node_ids: list[uuid.UUID] | None
    is_active: bool
    version: int
    renotify_interval_seconds: int
    cooldown_seconds: int
    created_at: datetime
    updated_at: datetime


class RuleCache:
    """Thread-safe in-memory rule cache.

    Rules are indexed by metric_field → list[CachedRule].
    Refreshed periodically from the database.

    Thread safety: all operations are async and run in a single
    event loop, so no explicit locking is needed.
    """

    def __init__(self):
        self._rules_by_field: dict[str, list[CachedRule]] = {}
        self._rules_by_id: dict[uuid.UUID, CachedRule] = {}
        self._refresh_count: int = 0
        self._last_refresh: float | None = None

    def get_rules_for_field(self, metric_field: str) -> list[CachedRule]:
        """Return all active rules matching a metric field.

        Returns empty list if no rules match (caller must handle).
        """
        return self._rules_by_field.get(metric_field, [])

    def get_by_id(self, rule_id: uuid.UUID) -> CachedRule | None:
        """Lookup a single rule by ID (used in re-notify loop)."""
        return self._rules_by_id.get(rule_id)

    def all_rules(self) -> list[CachedRule]:
        """Return all cached rules (used in startup recovery)."""
        return list(self._rules_by_id.values())

    async def refresh(self) -> None:
        """Load all active rules from DB and rebuild the index.

        Called periodically by the scheduler. Also called on
        worker startup.
        """
        session: AsyncSession
        async with async_session_factory() as session:
            try:
                result = await session.execute(
                    select(AlertRule).where(
                        AlertRule.is_active.is_(True)
                    ).order_by(AlertRule.id)
                )
                rules = result.scalars().all()
            except Exception as exc:
                logger.error(
                    "rule_cache_refresh_failed",
                    error=str(exc),
                )
                return

        new_by_field: dict[str, list[CachedRule]] = {}
        new_by_id: dict[uuid.UUID, CachedRule] = {}

        for rule in rules:
            cached = CachedRule(
                id=rule.id,
                name=rule.name,
                metric_field=rule.metric_field,
                operator=rule.operator,
                threshold=rule.threshold,
                duration_seconds=rule.duration_seconds or 0,
                channels=rule.channels or [],
                node_ids=rule.node_ids,
                is_active=rule.is_active,
                version=getattr(rule, 'version', 1),
                renotify_interval_seconds=getattr(rule, 'renotify_interval_seconds', 900),
                cooldown_seconds=getattr(rule, 'cooldown_seconds', 60),
                created_at=rule.created_at,
                updated_at=rule.updated_at,
            )
            new_by_id[rule.id] = cached

            field = rule.metric_field
            if field not in new_by_field:
                new_by_field[field] = []
            new_by_field[field].append(cached)

        self._rules_by_field = new_by_field
        self._rules_by_id = new_by_id
        self._refresh_count += 1
        self._last_refresh = datetime.now(timezone.utc).timestamp()

        if self._refresh_count == 1:
            logger.info(
                "rule_cache_initialized",
                rule_count=len(new_by_id),
                field_count=len(new_by_field),
            )
        elif self._refresh_count % 10 == 0:
            logger.debug(
                "rule_cache_refreshed",
                rule_count=len(new_by_id),
                refresh_count=self._refresh_count,
            )

    @property
    def is_loaded(self) -> bool:
        return self._last_refresh is not None


# Singleton — imported by worker
rule_cache = RuleCache()
