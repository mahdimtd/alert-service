import asyncio  
import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from sqlalchemy import select

from src.config import settings
from src.database import SessionLocal
from src.models.outbox_events import OutboxEvent

logger = logging.getLogger(__name__)

BATCH_SIZE = settings.RELAY_BATCH_SIZE


async def relay_once(producer: AIOKafkaProducer) -> int:
    """Publish one batch of unpublished outbox rows. Returns how many were sent."""
    db = SessionLocal()
    try:
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(BATCH_SIZE)
        )
        events = db.execute(stmt).scalars().all()
        logger.debug("Polled outbox: found %d events", len(events))
        if not events:
            return 0

        # Fire every publish without waiting; aiokafka coalesces them into
        # producer-side batches per partition. Keyed by aggregate_id so all
        # events of one alert stay ordered on the same partition.
        futures = []
        for event in events:
            message = {
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "aggregate_type": event.aggregate_type,
                "aggregate_id": event.aggregate_id,
                "payload": event.payload,
                "created_at": event.created_at.isoformat(),
            }
            futures.append((
                event,
                producer.send(
                    settings.OUTBOX_TOPIC,
                    key=str(event.aggregate_id).encode(),
                    value=json.dumps(message).encode(),
                ),
            ))

        results = await asyncio.gather(*(fut for _, fut in futures), return_exceptions=True)

        published_at = datetime.now(timezone.utc)
        sent = failed = 0
        for (event, _), result in zip(futures, results):
            if isinstance(result, BaseException):
                failed += 1
                logger.error("Publish failed for outbox event %s: %r", event.id, result)
                continue  # stays published_at NULL -> retried on a later cycle
            event.published_at = published_at
            sent += 1

        db.commit()
        if failed:
            logger.warning("Published %d/%d events (%d failed, will retry)", sent, len(events), failed)
        else:
            logger.info("Published %d events to Kafka", sent)
        return sent
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


async def run_relay() -> None:
    """
    Long-running loop: publish outbox_events rows to the shared Kafka
    topic. Runs as its own process, separate from the FastAPI app and
    the readings consumer:

        python -m src.kafka.relay

    NOTE on delivery semantics: published_at is set right after
    send_and_wait succeeds, committed in the same db transaction as the
    batch. If the process crashes between the Kafka send succeeding and
    the db commit, that row gets re-sent on the next pass (at-least-once,
    not exactly-once) -- downstream consumers should de-duplicate on
    (aggregate_type, aggregate_id, event_type, created_at) if that
    matters to them.
    """
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    logger.info("Alert relay started, polling every %ds", settings.OUTBOX_POLL_INTERVAL_SECONDS)
    try:
        while True:
            sent = await relay_once(producer)
            if sent == 0:
                logger.debug("No pending events, sleeping %ds", settings.OUTBOX_POLL_INTERVAL_SECONDS)
                await asyncio.sleep(settings.OUTBOX_POLL_INTERVAL_SECONDS)
    finally:
        await producer.stop()
        logger.info("Alert relay stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s'
    )
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logger.setLevel(logging.DEBUG)
    asyncio.run(run_relay())