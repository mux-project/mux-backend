"""Alert Engine Worker — standalone service entry point.

Runs the stream consumer, rule cache refresher, and periodic
re-notify/watchdog loop. Connects to Redis + PostgreSQL on boot.

Lifecycle:
  1. Connect to Redis
  2. Create StateManager
  3. Rebuild Redis state from DB (startup recovery)
  4. Start rule cache refresher (every 30s)
  5. Start re-notify/watchdog loop (every 60s)
  6. Start stream consumer (blocking)
"""

import asyncio
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from functools import partial

from prometheus_client import start_http_server

from app.alerts import metrics
from app.alerts.cache import rule_cache
from app.alerts.consumer import StreamConsumer
from app.alerts.db_writer import delete_breach_window_db, drain_notification_tasks, fire_alert, persist_breach_window, resolve_alert
from app.alerts.evaluator import evaluate_metric
from app.alerts.recovery import rebuild_state_from_db
from app.alerts.renotify import renotify_loop
from app.alerts.state import StateManager
from app.config import settings
from app.core.logging import configure_logging, logger
from app.core.redis import close_redis, get_redis
from app.database.session import async_session_factory


def _parse_metric_fields(data: dict) -> list[tuple[str, float]]:
    """Extract (metric_field, value) pairs from any metric data dict.

    Iterates all keys and extracts any numeric (int/float) value.
    Non-metric fields (interface, pid, name, etc.) yield no rule
    matches because no AlertRule will reference them.
    """
    return [
        (field, float(val))
        for field, val in data.items()
        if isinstance(val, (int, float))
    ]


def _parse_collected_at(collected_at_str: str | None) -> float:
    """Parse collected_at timestamp, returning Redis-compatible float."""
    if not collected_at_str:
        return None
    try:
        dt = datetime.fromisoformat(collected_at_str.replace("Z", "+00:00"))
        return dt.timestamp()
    except (ValueError, TypeError):
        return None


async def process_metric(
    redis,
    node_id: str,
    metric_type: str,
    data: dict,
    collected_at: str,
    tenant_id: str,
) -> None:
    """Callback invoked by StreamConsumer for each metric event.

    Full evaluation pipeline:
      1. Extract metric_field/value pairs from data
      2. Look up matching rules in cache
      3. Evaluate each rule against each metric field
      4. Fire or resolve based on evaluation decision
      5. Dispatch notifications for new fires

    All workers share a single Redis connection pool — the `redis`
    instance is the shared singleton from get_redis().
    """
    if not node_id or not data:
        logger.debug("empty_data_payload", node_id=node_id)
        metrics.alert_empty_payload.inc()
        return

    state = StateManager(redis, tenant=tenant_id)
    parsed_node_id = uuid.UUID(node_id) if isinstance(node_id, str) else node_id
    collected_ts = _parse_collected_at(collected_at)

    _iter = 0
    async with async_session_factory() as db_session:
        for metric_field, value in _parse_metric_fields(data):
            rules = rule_cache.get_rules_for_field(metric_field)
            if not rules:
                continue

            for rule in rules:
                _iter += 1
                if _iter % 50 == 0:
                    await asyncio.sleep(0)
                try:
                    with metrics.alert_evaluation_duration.time():
                        decision = await evaluate_metric(
                            state=state,
                            rule=rule,
                            node_id=parsed_node_id,
                            value=value,
                            collected_at=collected_ts,
                        )

                    action = decision.get("action")
                    metrics.alert_evaluations.labels(result=action).inc()

                    if action == "fire":
                        await delete_breach_window_db(rule.id, parsed_node_id, tenant_id=tenant_id, db_session=db_session)
                        await fire_alert(
                            state=state,
                            rule=rule,
                            node_id=parsed_node_id,
                            value=value,
                            tenant_id=tenant_id,
                        )

                    elif action == "resolve":
                        active_alert = await state.get_active_alert(rule.id, parsed_node_id)
                        if active_alert:
                            await resolve_alert(
                                state=state,
                                rule=rule,
                                node_id=parsed_node_id,
                                active_alert=active_alert,
                                tenant_id=tenant_id,
                            )

                    elif action == "breach_start":
                        started_at = decision.get("started_at")
                        if started_at:
                            await persist_breach_window(rule.id, parsed_node_id, started_at, tenant_id=tenant_id, db_session=db_session)

                    elif action == "breach_cleared":
                        await delete_breach_window_db(rule.id, parsed_node_id, tenant_id=tenant_id, db_session=db_session)

                except Exception as exc:
                    logger.exception(
                        "rule_evaluation_error",
                        rule_id=str(rule.id),
                        rule_name=rule.name,
                        node_id=str(parsed_node_id),
                        metric_field=metric_field,
                        value=value,
                        error=str(exc),
                    )


async def _rule_cache_refresher(stop_event: asyncio.Event) -> None:
    """Periodically refresh the rule cache from the database."""
    while True:
        try:
            if stop_event.is_set():
                break
            await asyncio.sleep(settings.ALERT_ENGINE_CACHE_REFRESH)
            await rule_cache.refresh()
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("rule_cache_refresher_error", error=str(exc))


async def _metrics_collector(redis, stop_event: asyncio.Event) -> None:
    """Periodically collect and set Prometheus gauges from Redis state."""
    while True:
        try:
            if stop_event.is_set():
                break
            await asyncio.sleep(30)

            # Active alert count — aggregate across ALL tenant sets (C2)
            total_active = 0
            async for set_key in redis.scan_iter(match="active_set*", count=100):
                total_active += await redis.scard(set_key)
            metrics.alert_active_count.set(total_active)

            # Breach window count — SCAN (non-blocking) instead of KEYS (C1)
            breach_count = 0
            async for _ in redis.scan_iter(match="breach:*", count=1000):
                breach_count += 1
            metrics.alert_breach_window_count.set(breach_count)

            # Cooldown count — SCAN (non-blocking) instead of KEYS (C1)
            cooldown_count = 0
            async for _ in redis.scan_iter(match="cooldown:*", count=1000):
                cooldown_count += 1
            metrics.alert_cooldown_count.set(cooldown_count)

            # Stream lag — XPENDING-based fallback for Redis < 7.0 (H3)
            try:
                info = await redis.xinfo_groups(settings.ALERT_ENGINE_STREAM_NAME)
                for g in info:
                    if g[b"name"] == settings.ALERT_ENGINE_CONSUMER_GROUP.encode():
                        pending = g.get(b"pending", 0)
                        metrics.alert_pending_messages.set(pending)
                        if pending > 0:
                            lag = g.get(b"lag")
                            if lag is not None:
                                metrics.alert_stream_lag.set(float(lag))
                            else:
                                # Redis < 7.0 fallback: estimate from oldest pending
                                try:
                                    pending_summary = await redis.xpending(
                                        settings.ALERT_ENGINE_STREAM_NAME,
                                        settings.ALERT_ENGINE_CONSUMER_GROUP,
                                    )
                                    if pending_summary and len(pending_summary) >= 2:
                                        oldest_id = pending_summary[1]
                                        if oldest_id:
                                            oldest_ms = int(oldest_id.split(b"-")[0])
                                            now_ms = int(time.time() * 1000)
                                            metrics.alert_stream_lag.set(
                                                max(0.0, (now_ms - oldest_ms) / 1000.0)
                                            )
                                except Exception:
                                    pass
                        else:
                            metrics.alert_stream_lag.set(0.0)
                        break
            except Exception:
                pass

            # DLQ size
            try:
                dlq_len = await redis.xlen(settings.ALERT_ENGINE_DLQ_NAME)
                metrics.alert_dlq_size.set(dlq_len)
            except Exception:
                pass

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("metrics_collector_error", error=str(exc))


async def run_worker(stop_event: asyncio.Event) -> None:
    """Main worker lifecycle."""
    logger.info(
        "alert_worker_starting",
        stream=settings.ALERT_ENGINE_STREAM_NAME,
        group=settings.ALERT_ENGINE_CONSUMER_GROUP,
    )

    # 0. Start Prometheus metrics HTTP server (port 8001 by default)
    metrics_port = getattr(settings, "ALERT_ENGINE_METRICS_PORT", 8001)
    try:
        start_http_server(metrics_port)
        logger.info("prometheus_metrics_server_started", port=metrics_port)
    except Exception as exc:
        logger.warning("prometheus_metrics_server_failed", error=str(exc), port=metrics_port)

    # 1. Connect to Redis
    redis = await get_redis()
    state = StateManager(redis)

    # 2. Startup recovery — rebuild Redis from DB
    await rule_cache.refresh()
    restored = await rebuild_state_from_db(state)
    logger.info("startup_recovery_done", active_alerts_restored=restored)

    # 3. Start rule cache refresher
    cache_task = asyncio.create_task(_rule_cache_refresher(stop_event))

    # 4. Start periodic metrics collector
    metrics_task = asyncio.create_task(_metrics_collector(redis, stop_event))

    # 5. Start re-notify + watchdog loop
    renotify_task = asyncio.create_task(
        renotify_loop(
            state=state,
            interval=settings.ALERT_ENGINE_RENOTIFY_INTERVAL,
            stop_event=stop_event,
        )
    )

    # 5. Start stream consumer
    # H1: All replicas MUST use the same consumer name for ordering.
    logger.info(
        "consumer_config",
        consumer_name=settings.ALERT_ENGINE_CONSUMER_NAME,
        consumer_group=settings.ALERT_ENGINE_CONSUMER_GROUP,
        ordering="enforced" if True else "best_effort",
    )
    consumer = StreamConsumer(
        stream=settings.ALERT_ENGINE_STREAM_NAME,
        dlq=settings.ALERT_ENGINE_DLQ_NAME,
        group=settings.ALERT_ENGINE_CONSUMER_GROUP,
        consumer=settings.ALERT_ENGINE_CONSUMER_NAME,
        max_retries=settings.ALERT_ENGINE_MAX_RETRIES,
        idempotency_ttl=settings.ALERT_ENGINE_IDEMPOTENCY_TTL,
    )
    consumer.set_process_callback(partial(process_metric, redis))

    try:
        await consumer.run_forever(stop_event)
    except Exception as exc:
        logger.exception("worker_crashed", error=str(exc))
        raise
    finally:
        logger.info("worker_shutting_down")
        cache_task.cancel()
        metrics_task.cancel()
        renotify_task.cancel()
        try:
            await asyncio.wait(
                [cache_task, metrics_task, renotify_task],
                timeout=10,
                return_when=asyncio.ALL_COMPLETED,
            )
        except Exception:
            pass
        await drain_notification_tasks(timeout=10)
        await close_redis()
        logger.info("worker_shutdown_complete")


def main():
    configure_logging()
    logger.info("alert_engine_worker_booting", app_env=settings.APP_ENV)

    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("worker_received_signal_shutting_down")
        stop_event.set()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig_name in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(signal, sig_name), _handle_signal)
        except (NotImplementedError, AttributeError):
            try:
                signal.signal(getattr(signal, sig_name), lambda *_: _handle_signal())
            except Exception:
                pass

    try:
        loop.run_until_complete(run_worker(stop_event))
    except KeyboardInterrupt:
        logger.info("worker_interrupted")
    finally:
        pending = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(
                asyncio.wait(pending, timeout=10, return_when=asyncio.ALL_COMPLETED)
            )
        loop.close()


if __name__ == "__main__":
    main()
