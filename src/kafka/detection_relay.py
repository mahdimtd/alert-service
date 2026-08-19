import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

from aiokafka import AIOKafkaProducer
from sqlalchemy import select

from src.config import settings
from src.database import SessionLocal
from src.models.detection_outbox_events import DetectionOutboxEvent

logger = logging.getLogger(__name__)

BATCH_SIZE = 100


def _backoff_seconds(attempt_count: int) -> int:
    # Exponential backoff, capped at 1 hour: base, 2*base, 4*base, ...
    base = settings.DETECTION_OUTBOX_BASE_BACKOFF_SECONDS
    return min(base * (2 ** max(attempt_count - 1, 0)), 3600)


async def relay_once(producer: AIOKafkaProducer) -> int:
    """Publish one batch of pending/retryable detection outbox rows. Returns how many were sent."""
    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        stmt = (
            select(DetectionOutboxEvent)
            .where(
                DetectionOutboxEvent.status.in_(("PENDING", "FAILED")),
                (DetectionOutboxEvent.next_retry_at.is_(None))
                | (DetectionOutboxEvent.next_retry_at <= now),
            )
            .order_by(DetectionOutboxEvent.id)
            .limit(BATCH_SIZE)
        )
        events = db.execute(stmt).scalars().all()
        logger.debug("Polled outbox: found %d events", len(events))
        if not events:
            return 0

        sent = 0
        for event in events:
            message = {
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "aggregate_type": event.aggregate_type,
                "aggregate_id": str(event.aggregate_id),
                "premises_id": event.premises_id,
                "rule_id": str(event.rule_id),
                "condition_key": event.payload.get("condition_key"),
                "payload": event.payload,
                "created_at": event.created_at.isoformat(),
            }
            try:
                await producer.send_and_wait(
                    event.topic,
                    key=event.partition_key.encode(),
                    value=json.dumps(message).encode(),
                )
            except Exception as exc:
                event.attempt_count += 1
                event.status = "FAILED"
                event.error_message = str(exc)[:2000]
                event.next_retry_at = now + timedelta(seconds=_backoff_seconds(event.attempt_count))
                if event.attempt_count >= settings.DETECTION_OUTBOX_MAX_ATTEMPTS:
                    logger.error(
                        "Detection outbox event %s exceeded max attempts (%s); "
                        "left as FAILED for manual review",
                        event.id, settings.DETECTION_OUTBOX_MAX_ATTEMPTS,
                    )
                continue

            event.status = "PUBLISHED"
            event.published_at = now
            event.attempt_count += 1
            event.next_retry_at = None
            event.error_message = None
            sent += 1

        db.commit()
        logger.info("Published %d events to Kafka", sent)
        return sent
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


async def run_relay() -> None:
    """
    Long-running loop: publish detection.outbox_events rows to whichever
    Kafka topic each row names (event.topic -- currently always
    settings.CONDITION_EVENTS_TOPIC). Runs as its own process, separate
    from the FastAPI app and both consumers:

        python -m src.kafka.detection_relay

    Unlike src/kafka/relay.py (the Alert domain's relay, which only has
    published_at), this table tracks a real status/retry lifecycle
    (PENDING -> PUBLISHED, or PENDING -> FAILED -> retried later with
    backoff), so a transient Kafka failure here doesn't need the whole
    process to crash -- the row is left FAILED with a next_retry_at and
    the relay moves on to the rest of the batch.
    """
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    logger.info("Detection relay started, polling every %ds", settings.DETECTION_OUTBOX_POLL_INTERVAL_SECONDS)
    try:
        while True:
            sent = await relay_once(producer)
            if sent == 0:
                logger.debug("No pending events, sleeping %ds", settings.DETECTION_OUTBOX_POLL_INTERVAL_SECONDS)
                await asyncio.sleep(settings.DETECTION_OUTBOX_POLL_INTERVAL_SECONDS)
    finally:
        await producer.stop()
        logger.info("Detection relay stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s'
    )
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logger.setLevel(logging.DEBUG)
    asyncio.run(run_relay())
