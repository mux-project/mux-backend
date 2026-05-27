"""Core rule evaluation logic for the Alert Engine.

Evaluates each metric value against cached rules, manages
duration-based breach windows, and triggers fire/resolve
decisions via the DB Writer.
"""

import asyncio
import operator
import uuid

from app.alerts.cache import CachedRule
from app.alerts.state import StateManager
from app.core.logging import logger

# Operator mapping from stored strings to Python operator functions
OPERATORS = {
    "gt": operator.gt,
    "lt": operator.lt,
    "gte": operator.ge,
    "lte": operator.le,
    "eq": operator.eq,
}

# --- Redis resilience helper (H10) ---

_RETRYABLE = (TimeoutError, ConnectionError)


async def redis_op_with_retry(fn, *args, retries=3, base_delay=0.05, **kwargs):
    """Execute a Redis operation with exponential backoff retry.

    Retries on TimeoutError and ConnectionError (transient failures).
    Propagates the last exception if all retries are exhausted.
    Does NOT retry on logical errors (e.g. WRONGTYPE, key not found).
    """
    for attempt in range(retries):
        try:
            return await fn(*args, **kwargs)
        except _RETRYABLE as e:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(base_delay * (2 ** attempt))


def threshold_breached(rule_op: str, value: float, threshold: float) -> bool:
    """Evaluate whether a single metric value breaches a threshold.

    Returns True if the condition is met (alert-worthy).
    Supports: gt, lt, gte, lte, eq.
    """
    op_func = OPERATORS.get(rule_op)
    if op_func is None:
        logger.error("unknown_operator", operator=rule_op)
        return False
    return op_func(value, threshold)


def node_in_scope(rule: CachedRule, node_id: uuid.UUID) -> bool:
    """Check if a rule applies to a given node.

    Rule scope rules:
      - node_ids == None → applies to ALL nodes (global rule)
      - node_ids == [...] → applies only if node_id is in the list
    """
    if rule.node_ids is None:
        return True
    return node_id in rule.node_ids


async def evaluate_metric(
    state: StateManager,
    rule: CachedRule,
    node_id: uuid.UUID,
    value: float,
    collected_at: float | None,
) -> dict:
    """Evaluate a single metric point against one rule.

    Returns a decision dict with the action to take:
      {"action": "none"}                — no action needed
      {"action": "fire", "value": ...}  — alert should fire
      {"action": "resolve"}             — alert should resolve
      {"action": "breach_start"}        — breach window started (too early to fire)
      {"action": "breach_continue"}     — still in breach window, not yet fire

    This is a pure decision engine — it does NOT perform any DB
    or Redis mutations itself. The caller (Evaluator) handles those.
    """
    decision: dict = {"action": "none"}

    # 1. Scope check
    if not node_in_scope(rule, node_id):
        return decision

    # 2. Evaluate threshold
    breached = threshold_breached(rule.operator, value, rule.threshold)

    # 3. Acquire distributed lock for this (rule, node) — prevents race
    #    between concurrent workers on breach windows and fire decisions (N2).
    token = await redis_op_with_retry(state.acquire_lock, rule.id, node_id)
    if token is None:
        return decision

    try:
        # 4. Check current state (under lock) — all Redis calls use retry (H10)
        active_alert = await redis_op_with_retry(state.get_active_alert, rule.id, node_id)
        in_cooldown = await redis_op_with_retry(state.is_in_cooldown, rule.id, node_id)
        breach_start = await redis_op_with_retry(state.get_breach_window, rule.id, node_id)

        if breached:
            if active_alert:
                return {"action": "none", "value": value}

            if in_cooldown:
                return {"action": "none", "value": value}

            if breach_start is None:
                redis_now = await redis_op_with_retry(state.redis_time)
                await redis_op_with_retry(state.create_breach_window, rule.id, node_id, redis_now)
                return {
                    "action": "breach_start",
                    "started_at": redis_now,
                    "value": value,
                    "duration_seconds": rule.duration_seconds,
                }

            elapsed = (await redis_op_with_retry(state.redis_time)) - breach_start
            if elapsed >= rule.duration_seconds:
                await redis_op_with_retry(state.delete_breach_window, rule.id, node_id)
                return {
                    "action": "fire",
                    "value": value,
                    "breach_duration": elapsed,
                    "fired_at": breach_start,
                }

            return {
                "action": "breach_continue",
                "elapsed": elapsed,
                "duration_seconds": rule.duration_seconds,
                "value": value,
            }

        else:
            if breach_start is not None:
                await redis_op_with_retry(state.delete_breach_window, rule.id, node_id)
                return {"action": "breach_cleared", "breach_duration": (await redis_op_with_retry(state.redis_time)) - breach_start}

            if active_alert:
                return {"action": "resolve", "value": value}

            return {"action": "none"}

    finally:
        try:
            await redis_op_with_retry(state.release_lock, rule.id, node_id, token)
        except Exception:
            logger.exception("release_lock_failed",
                             rule_id=str(rule.id), node_id=str(node_id))
