import asyncio
import json

from app.alerts import metrics
from app.config import settings
from app.core.logging import logger
from app.core.redis import get_redis


class StreamConsumer:
    """Redis Stream consumer using consumer groups.

    Reads from `metrics:ingested`, handles idempotency, retries,
    and routes permanently-failed messages to `metrics:dead-letter`.

    Never XACKs on failure — pending messages are re-delivered
    to another consumer in the group via XAUTOCLAIM.

    Ordering guarantee (H1):
    All workers in a consumer group must use the SAME consumer name
    to preserve message ordering. Redis Streams consumer groups
    deliver each message to exactly one consumer — with different
    consumer names, messages for the same (rule, node) may be
    delivered to different workers and processed out of order,
    causing fire/resolve reversals.

    Set ALERT_ENGINE_CONSUMER_NAME to the same value across all
    worker replicas for ordered delivery.
    """

    def __init__(
        self,
        stream: str = settings.ALERT_ENGINE_STREAM_NAME,
        dlq: str = settings.ALERT_ENGINE_DLQ_NAME,
        group: str = settings.ALERT_ENGINE_CONSUMER_GROUP,
        consumer: str = settings.ALERT_ENGINE_CONSUMER_NAME,
        max_retries: int = settings.ALERT_ENGINE_MAX_RETRIES,
        idempotency_ttl: int = settings.ALERT_ENGINE_IDEMPOTENCY_TTL,
        read_count: int = 10,
        claim_count: int = 50,
        poll_interval: float = 1.0,
    ):
        self.stream = stream
        self.dlq = dlq
        self.group = group
        self.consumer = consumer
        self.max_retries = max_retries
        self.idempotency_ttl = idempotency_ttl
        self.read_count = read_count
        self.claim_count = claim_count
        self.poll_interval = poll_interval
        self._running = False
        self._process_callback = None

    def set_process_callback(self, callback):
        """Set the async callback invoked for each message.

        The callback receives a dict with the parsed message fields.
        It must return True on success or raise on failure.
        """
        self._process_callback = callback

    async def _ensure_group(self, r):
        """Create consumer group if it doesn't exist.

        XGROUP CREATE raises BUSYGROUP if it already exists;
        we catch and ignore.
        """
        try:
            await r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
            logger.info("consumer_group_created", stream=self.stream, group=self.group)
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug("consumer_group_exists", stream=self.stream, group=self.group)
            else:
                logger.warning("consumer_group_create_error", error=str(exc))

    async def _recover_pending(self, r):
        """Reclaim pending messages from dead consumers.

        XAUTOCLAIM reassigns pending messages that were not
        processed (crashed worker) to this consumer.
        """
        try:
            claimed, _ = await r.xautoclaim(
                self.stream,
                self.group,
                self.consumer,
                min_idle_time=300000,  # 5 min idle → reclaim (M16)
                count=self.claim_count,
            )
            if claimed:
                logger.info("claimed_pending_messages", count=len(claimed))
        except Exception as exc:
            logger.warning("xautoclaim_error", error=str(exc))

    async def _move_to_dlq(self, r, msg_id_str, msg_data, error_info):
        """Move a permanently-failed message to the dead-letter queue."""
        # H8: clear retry key so DLQ replay doesn't loop back immediately
        tenant_id = msg_data.get("tenant_id", "")
        retry_key = f"retry:{tenant_id}:{msg_id_str}" if tenant_id else f"retry:{msg_id_str}"
        await r.delete(retry_key)
        logger.debug("dlq_retry_key_cleared", msg_id=msg_id_str, retry_key=retry_key)

        dlq_entry = {
            "original_msg_id": msg_id_str,
            "tenant_id": msg_data.get("tenant_id", ""),
            "node_id": msg_data.get("node_id", ""),
            "payload": json.dumps(msg_data),
            "error": str(error_info.get("error", "")),
            "retry_count": str(error_info.get("retries", self.max_retries)),
            "failed_at": str(asyncio.get_event_loop().time()),
        }
        await r.xadd(self.dlq, dlq_entry, maxlen=10000, approximate=True)
        metrics.alert_dlq_entries.inc()
        logger.warning(
            "message_sent_to_dlq",
            msg_id=msg_id_str,
            stream=self.dlq,
            error=error_info.get("error", ""),
            retries=error_info.get("retries", self.max_retries),
        )

    async def _check_idempotency(self, r, msg_id_str, tenant_id: str = ""):
        """Check if this message was already processed successfully.

        Returns True if already processed (skip+ACK).
        Does NOT set the key — caller must call _mark_processed after
        successful processing to avoid breaking retries (C1).
        Key includes tenant_id to prevent cross-tenant collisions.
        """
        key = f"processed:{tenant_id}:{msg_id_str}" if tenant_id else f"processed:{msg_id_str}"
        return await r.exists(key)

    async def _mark_processed(self, r, msg_id_str, tenant_id: str = ""):
        """Mark a message as successfully processed (called AFTER callback success).

        Sets a TTL-scoped key so future duplicates are skipped.
        This must only be called AFTER the processing callback succeeds,
        otherwise retries are permanently blocked (C1 fix).
        """
        key = f"processed:{tenant_id}:{msg_id_str}" if tenant_id else f"processed:{msg_id_str}"
        await r.setex(key, self.idempotency_ttl, "1")

    async def _check_retries(self, r, msg_id_str, tenant_id: str = ""):
        """Check and increment retry count.

        Returns (retry_count, should_dlq).
        Key includes tenant_id to prevent cross-tenant collisions.
        """
        key = f"retry:{tenant_id}:{msg_id_str}" if tenant_id else f"retry:{msg_id_str}"
        retries = await r.incr(key)
        if retries == 1:
            await r.expire(key, self.idempotency_ttl)
        if retries > 1:
            metrics.alert_retries.inc()
        return retries, retries >= self.max_retries

    async def _process_single(self, r, msg_id_str, msg_data):
        """Process one stream message with full safety interlocks."""
        # 1. Validate message structure
        if not isinstance(msg_data, dict) or "msg_id" not in msg_data:
            logger.error("malformed_message", msg_id=msg_id_str, data=msg_data)
            await self._move_to_dlq(r, msg_id_str, msg_data, {"error": "malformed_message"})
            return True  # ACK the invalid message (moved to DLQ)

        tenant_id = msg_data.get("tenant_id", "")

        # 2. Idempotency check (24h window)
        if await self._check_idempotency(r, msg_data.get("msg_id", msg_id_str), tenant_id):
            logger.debug("message_already_processed", msg_id=msg_id_str)
            metrics.alert_messages_consumed.labels(status="skip").inc()
            return True  # ACK and skip

        # 3. Retry check
        retries, should_dlq = await self._check_retries(r, msg_id_str, tenant_id)
        if should_dlq:
            logger.warning("message_max_retries", msg_id=msg_id_str, retries=retries)
            await self._move_to_dlq(r, msg_id_str, msg_data, {"error": "max_retries_exceeded", "retries": retries})
            return True  # ACK and move on

        # 4. Process (callback raises on failure)
        if self._process_callback:
            # Parse data JSON if string
            data = msg_data.get("data", "{}")
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    data = {}

            try:
                await self._process_callback(
                    node_id=msg_data.get("node_id"),
                    metric_type=msg_data.get("metric_type", "system"),
                    data=data,
                    collected_at=msg_data.get("collected_at"),
                    tenant_id=msg_data.get("tenant_id", ""),
                )
            except Exception:
                # Do NOT mark as processed — retry will re-invoke callback (C1)
                raise

        # 5. Mark as processed (only after callback succeeds)
        # Must not prevent XACK on Redis failure — message is idempotent at app layer
        try:
            await self._mark_processed(r, msg_data.get("msg_id", msg_id_str), tenant_id)
        except Exception:
            logger.exception("mark_processed_failed", msg_id=msg_id_str)

        return True  # ACK

    async def run_once(self, r):
        """Read one batch from the stream and process messages."""
        results = await r.xreadgroup(
            group=self.group,
            consumername=self.consumer,
            streams={self.stream: ">"},
            count=self.read_count,
            block=1000,
        )

        if not results:
            return 0

        processed = 0
        for stream_name, messages in results:
            for msg_id, msg_data in messages:
                try:
                    ok = await self._process_single(r, msg_id, msg_data)
                    if ok:
                        await r.xack(self.stream, self.group, msg_id)
                        processed += 1
                        metrics.alert_messages_consumed.labels(status="ack").inc()
                except Exception as exc:
                    logger.exception(
                        "message_processing_failed",
                        msg_id=msg_id,
                        error=str(exc),
                    )
                    metrics.alert_messages_consumed.labels(status="error").inc()
                    # Do NOT XACK — message stays pending for re-delivery

        return processed

    async def run_forever(self, stop_event: asyncio.Event | None = None):
        """Main consumer loop. Reclaims pending on start, then continuously reads."""
        logger.info(
            "consumer_starting",
            stream=self.stream,
            group=self.group,
            consumer=self.consumer,
        )
        self._running = True

        r = await get_redis()

        # Ensure consumer group exists
        await self._ensure_group(r)

        # Recover pending messages from crashed workers
        await self._recover_pending(r)

        # Main loop
        self._running = True
        claim_counter = 0
        while self._running:
            try:
                if stop_event and stop_event.is_set():
                    break

                # Verify Redis connection is alive
                try:
                    await r.ping()
                except Exception:
                    logger.critical("redis_connection_lost", stream=self.stream)
                    metrics.alert_redis_errors.labels(operation="connection").inc()
                    await asyncio.sleep(5)
                    r = await get_redis()
                    continue

                # H4: periodic XAUTOCLAIM to prevent PEL growth from dead consumers
                claim_counter += 1
                if claim_counter % 60 == 0:
                    await self._recover_pending(r)

                processed = await self.run_once(r)

                # Brief sleep when no messages to avoid busy-loop
                if processed == 0:
                    await asyncio.sleep(self.poll_interval)

            except asyncio.CancelledError:
                logger.info("consumer_cancelled")
                break
            except Exception as exc:
                logger.exception("consumer_loop_error", error=str(exc))
                await asyncio.sleep(5)

        logger.info("consumer_stopped", stream=self.stream)

    def stop(self):
        self._running = False
