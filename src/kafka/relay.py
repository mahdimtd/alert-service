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

BATCH_SIZE = 100


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
        if not events:
            return 0

        for event in events:
            message = {
                "event_type": event.event_type,
                "aggregate_type": event.aggregate_type,
                "aggregate_id": event.aggregate_id,
                "payload": event.payload,
                "created_at": event.created_at.isoformat(),
            }
            # Key by aggregate_id so every event for the same alert/
            # notification lands on the same partition and stays ordered.
            await producer.send_and_wait(
                settings.OUTBOX_TOPIC,
                key=str(event.aggregate_id).encode(),
                value=json.dumps(message).encode(),
            )
            event.published_at = datetime.now(timezone.utc)

        db.commit()
        return len(events)
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
    try:
        while True:
            sent = await relay_once(producer)
            if sent == 0:
                await asyncio.sleep(settings.OUTBOX_POLL_INTERVAL_SECONDS)
    finally:
        await producer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_relay())