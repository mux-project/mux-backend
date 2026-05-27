"""DLQ Replay Worker — reads from the dead-letter stream and
attempts to reprocess messages.

Runs as a periodic background job (every 5 minutes).
Messages that still fail after MAX_RETRIES remain in DLQ
for manual inspection.
"""

import asyncio
import json

from app.alerts.consumer import StreamConsumer
from app.config import settings
from app.core.logging import logger
from app.core.redis import get_redis

# Messages that fail DLQ replay this many times are left
# in the DLQ for manual handling (poison messages).
DLQ_MAX_RETRIES = 3
DLQ_BATCH_SIZE = 10


async def replay_dlq(
    consumer: StreamConsumer | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Periodically read from the dead-letter queue and attempt reprocessing.

    Runs every 300 seconds (5 min). For each DLQ message:
      1. Extract original msg_id and payload
      2. Delete retry counter so it gets 3 fresh retries
      3. Fork message back to the main stream
      4. XDEL from DLQ
      5. If fork fails (still failing), leave in DLQ
    """
    logger.info("dlq_replay_worker_starting")

    while True:
        try:
            if stop_event and stop_event.is_set():
                break

            await asyncio.sleep(300)
            await _run_dlq_replay_cycle()

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("dlq_replay_error", error=str(exc))

    logger.info("dlq_replay_worker_stopped")


async def _run_dlq_replay_cycle() -> None:
    """Read one batch of messages from DLQ and attempt reprocessing."""
    r = await get_redis()
    dlq = settings.ALERT_ENGINE_DLQ_NAME
    stream_name = settings.ALERT_ENGINE_STREAM_NAME

    # Read oldest messages from DLQ
    results = await r.xread(
        streams={dlq: "0-0"},
        count=DLQ_BATCH_SIZE,
        block=1000,
    )

    if not results:
        return

    replayed = 0
    for stream_name_inner, messages in results:
        for msg_id, msg_data in messages:
            try:
                original_msg_id = msg_data.get("original_msg_id", "")
                tenant_id = msg_data.get("tenant_id", "")
                payload_raw = msg_data.get("payload", "{}")

                # Clear retry counter so message gets fair retry
                if original_msg_id:
                    retry_key = f"retry:{tenant_id}:{original_msg_id}" if tenant_id else f"retry:{original_msg_id}"
                    await r.delete(retry_key)

                # Re-fork the original payload back to the main stream
                if isinstance(payload_raw, str):
                    payload = json.loads(payload_raw)
                else:
                    payload = payload_raw

                await r.xadd(
                    stream_name,
                    payload,
                    maxlen=settings.ALERT_ENGINE_MAXLEN,
                    approximate=True,
                )

                # Remove from DLQ
                await r.xdel(dlq, msg_id)
                replayed += 1

                logger.info(
                    "dlq_message_replayed",
                    original_msg_id=original_msg_id,
                    dlq_msg_id=msg_id,
                )

            except Exception as exc:
                # If DLQ replay itself fails, leave the message in DLQ
                logger.warning(
                    "dlq_replay_failed",
                    dlq_msg_id=msg_id,
                    error=str(exc),
                )

    if replayed > 0:
        logger.info("dlq_replay_cycle_complete", replayed=replayed)
