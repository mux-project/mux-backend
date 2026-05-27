"""Redis state management for the Alert Engine.

Provides abstractions over Redis keys for:
  - Breach windows (duration tracking)
  - Active alerts (currently firing)
  - Cooldowns (post-resolution hold-off)
  - Distributed locks (fire/resolve mutual exclusion)

All timestamps use redis.time() for clock consistency across workers.
"""

import secrets
import time as time_module
import uuid

from app.config import settings
from app.core.logging import logger


class StateManager:
    """Central state management for alert evaluation.

    Every interaction with Redis for alert state goes through
    this class. This provides a single point of change for
    key naming conventions, serialization, and tenancy.
    """

    def __init__(self, redis, tenant: str = ""):
        self._r = redis
        self._tenant = tenant or ""

    @staticmethod
    def _uuid_key(u: uuid.UUID) -> str:
        """Normalize UUID to 32-char hex string for consistent Redis keys."""
        return u.hex

    def _key(self, prefix: str, *parts: str) -> str:
        """Build a qualified Redis key with tenant prefix."""
        return f"{prefix}:{self._tenant}:{':'.join(parts)}" if self._tenant else f"{prefix}:{':'.join(parts)}"

    def _active_set_key(self) -> str:
        """Key for the SMEMBERS-based active alert index (F11)."""
        return f"active_set:{self._tenant}" if self._tenant else "active_set"

    # --- Clock ---

    async def redis_time(self) -> float:
        """Return current time from Redis server as float seconds."""
        seconds, microseconds = await self._r.time()
        return float(f"{seconds}.{microseconds:06d}")

    # --- Distributed Locks ---

    async def acquire_lock(
        self, rule_id: uuid.UUID, node_id: uuid.UUID, ttl: int | None = None
    ) -> str | None:
        """Acquire a distributed lock for evaluating (rule, node).

        Returns a lock token if acquired, None if another worker holds it.
        The lock auto-releases after `ttl` seconds.
        Token must be stored for safe release.
        """
        token = secrets.token_hex(16)
        key = self._key("lock:eval", self._uuid_key(rule_id), self._uuid_key(node_id))
        ttl = ttl or settings.ALERT_ENGINE_LOCK_TTL
        acquired = await self._r.set(key, token, nx=True, ex=ttl)
        return token if acquired else None

    async def release_lock(
        self, rule_id: uuid.UUID, node_id: uuid.UUID, token: str
    ) -> bool:
        """Safely release a distributed lock using a LUA script.

        Only deletes the key if the token matches (safe release).
        Returns True if the lock was released, False if it was
        already expired or owned by another worker.
        """
        key = self._key("lock:eval", self._uuid_key(rule_id), self._uuid_key(node_id))
        lua = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        end
        return 0
        """
        result = await self._r.eval(lua, 1, key, token)
        return bool(result)

    # --- Breach Windows ---

    async def create_breach_window(
        self, rule_id: uuid.UUID, node_id: uuid.UUID, started_at: float
    ) -> None:
        """Record the start of a threshold breach for duration tracking."""
        key = self._key("breach", self._uuid_key(rule_id), self._uuid_key(node_id))
        await self._r.hset(key, mapping={"started_at": str(started_at)})

    async def get_breach_window(
        self, rule_id: uuid.UUID, node_id: uuid.UUID
    ) -> float | None:
        """Return breach started_at timestamp, or None if no active window."""
        key = self._key("breach", self._uuid_key(rule_id), self._uuid_key(node_id))
        val = await self._r.hget(key, "started_at")
        return float(val) if val else None

    async def delete_breach_window(
        self, rule_id: uuid.UUID, node_id: uuid.UUID
    ) -> None:
        """Clear a breach window (condition returned to normal, or after firing)."""
        key = self._key("breach", self._uuid_key(rule_id), self._uuid_key(node_id))
        await self._r.delete(key)

    # --- Active Alerts ---

    async def set_active_alert(
        self,
        rule_id: uuid.UUID,
        node_id: uuid.UUID,
        alert_history_id: uuid.UUID,
        rule_version: int,
        value: float,
        fired_at: float,
        last_notified_at: float | None = None,
        notified_count: int = 0,
    ) -> None:
        """Record an alert as currently firing in Redis.

        Also adds the key to the active set for O(1) iteration (F11).
        Initialises notified_count to 0 for rate-limiting (F13).
        """
        key = self._key("active", self._uuid_key(rule_id), self._uuid_key(node_id))
        await self._r.hset(key, mapping={
            "alert_history_id": str(alert_history_id),
            "rule_version": str(rule_version),
            "value": str(value),
            "fired_at": str(fired_at),
            "last_notified_at": str(last_notified_at or fired_at),
            "notified_count": str(notified_count),
        })
        await self._r.sadd(self._active_set_key(), key)

    async def get_active_alert(
        self, rule_id: uuid.UUID, node_id: uuid.UUID
    ) -> dict | None:
        """Return the active alert state dict, or None if not firing.

        Returns parsed dict with keys: alert_history_id, rule_version,
        value, fired_at, last_notified_at, notified_count.
        """
        key = self._key("active", self._uuid_key(rule_id), self._uuid_key(node_id))
        raw = await self._r.hgetall(key)
        if not raw:
            return None
        return {
            "alert_history_id": uuid.UUID(raw["alert_history_id"]),
            "rule_version": int(raw["rule_version"]),
            "value": float(raw["value"]),
            "fired_at": float(raw["fired_at"]),
            "last_notified_at": float(raw["last_notified_at"]),
            "notified_count": int(raw.get("notified_count", 0)),
        }

    async def delete_active_alert(
        self, rule_id: uuid.UUID, node_id: uuid.UUID
    ) -> None:
        """Remove an active alert from Redis (resolved or cleaned up).

        Also removes from the active set for consistent iteration (F11).
        """
        key = self._key("active", self._uuid_key(rule_id), self._uuid_key(node_id))
        await self._r.srem(self._active_set_key(), key)
        await self._r.delete(key)

    async def update_last_notified(
        self, rule_id: uuid.UUID, node_id: uuid.UUID, notified_at: float
    ) -> int:
        """Update last_notified_at and increment notified_count atomically (H7).

        Uses a LUA script to prevent partial updates if a crash occurs
        between the HSET and HINCRBY commands.

        Returns the new notified_count after increment.
        """
        key = self._key("active", self._uuid_key(rule_id), self._uuid_key(node_id))
        lua = """
        redis.call("HSET", KEYS[1], "last_notified_at", ARGV[1])
        return redis.call("HINCRBY", KEYS[1], "notified_count", 1)
        """
        new_count = await self._r.eval(lua, 1, key, str(notified_at))
        return new_count

    async def scan_active_alerts(self) -> list[tuple[uuid.UUID, uuid.UUID, dict]]:
        """Return all active alerts via SMEMBERS for O(1) iteration (F11).

        Uses the active set instead of SCAN for reliable, predictable
        performance at scale. Used by re-notify loop and ghost detection.
        """
        results = []
        set_key = self._active_set_key()
        member_keys = await self._r.smembers(set_key)

        for key in member_keys:
            raw = await self._r.hgetall(key)
            if not raw:
                continue
            # Extract rule_id and node_id from key (hex format, last 2 parts)
            parts = key.split(":")
            rule_id_hex = parts[-2]
            node_id_hex = parts[-1]
            results.append((
                uuid.UUID(hex=rule_id_hex),
                uuid.UUID(hex=node_id_hex),
                {
                    "alert_history_id": uuid.UUID(raw["alert_history_id"]),
                    "rule_version": int(raw["rule_version"]),
                    "value": float(raw["value"]),
                    "fired_at": float(raw["fired_at"]),
                    "last_notified_at": float(raw["last_notified_at"]),
                    "notified_count": int(raw.get("notified_count", 0)),
                },
            ))
        return results

    # --- Cooldowns ---

    async def set_cooldown(
        self, rule_id: uuid.UUID, node_id: uuid.UUID, duration: int
    ) -> None:
        """Set a cooldown with auto-expiry.

        During cooldown, the alert engine will not fire for
        this (rule, node) pair.
        """
        key = self._key("cooldown", self._uuid_key(rule_id), self._uuid_key(node_id))
        await self._r.setex(key, duration, "1")

    async def is_in_cooldown(
        self, rule_id: uuid.UUID, node_id: uuid.UUID
    ) -> bool:
        """Check if (rule, node) is currently in cooldown."""
        key = self._key("cooldown", self._uuid_key(rule_id), self._uuid_key(node_id))
        return await self._r.exists(key) > 0

    async def delete_cooldown(
        self, rule_id: uuid.UUID, node_id: uuid.UUID
    ) -> None:
        """Manually clear a cooldown (used during recovery/cleanup)."""
        key = self._key("cooldown", self._uuid_key(rule_id), self._uuid_key(node_id))
        await self._r.delete(key)
