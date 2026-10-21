import asyncio  
import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from sqlalchemy import select

from src.config import settings
from src.database import SessionLocal
from src.models.outbox_events import OutboxEvent

logger = logging.getLogger("alert-output-worker")

BATCH_SIZE = settings.RELAY_BATCH_SIZE


async def _delivery_result(pending) -> BaseException | None:
    """
    Resolve one enqueued message to None (broker acknowledged it) or to
    the exception that prevented delivery.
    """
    if isinstance(pending, BaseException):
        return pending
    try:
        await pending  # delivery future: resolves only after the broker ack
        return None
    except Exception as exc:
        return exc


def _poll_pending(db) -> list[OutboxEvent]:
    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.id)
        .limit(BATCH_SIZE)
    )
    return db.execute(stmt).scalars().all()


async def relay_once(producer: AIOKafkaProducer) -> int:
    """Publish one batch of unpublished outbox rows. Returns how many were sent."""
    db = SessionLocal()
    try:
        # Synchronous (psycopg2) DB read -- off the event loop via
        # asyncio.to_thread so a slow query or a row lock only blocks a
        # worker thread, not this process's whole async loop.
        events = await asyncio.to_thread(_poll_pending, db)
        logger.debug("Polled outbox: found %d events", len(events))
        if not events:
            return 0

        # Enqueue messages sequentially so per-partition order matches the
        # outbox order. `await producer.send()` only puts the message into
        # the producer batch and returns a delivery future; the broker ack
        # is awaited below.
        enqueued = []
        for event in events:
            message = {
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "aggregate_type": event.aggregate_type,
                # aggregate_id intentionally omitted from the wire format --
                # consumers identify the subject via payload.alert_id.
                # (still used below as the partition key)
                "payload": event.payload,
                "created_at": event.created_at.isoformat(),
            }
            try:
                delivery = await producer.send(
                    settings.OUTBOX_TOPIC,
                    key=str(event.aggregate_id).encode(),
                    value=json.dumps(message).encode(),
                )
            except Exception as exc:
                delivery = exc
            enqueued.append((event, delivery))

        # Wait for real broker acknowledgements before marking anything as
        # published.
        results = await asyncio.gather(*(_delivery_result(d) for _, d in enqueued))

        published_at = datetime.now(timezone.utc)
        sent = failed = 0
        for (event, _), result in zip(enqueued, results):
            if result is not None:
                failed += 1
                logger.error("Publish failed for outbox event %s: %r", event.id, result)
                continue  # stays published_at NULL -> retried on a later cycle
            event.published_at = published_at
            sent += 1

        # Synchronous (psycopg2) commit -- also off the event loop.
        await asyncio.to_thread(db.commit)
        if failed:
            logger.warning("Published %d/%d events (%d failed, will retry)", sent, len(events), failed)
        else:
            logger.info("Published %d events to Kafka", sent)
        return sent
    except Exception:
        await asyncio.to_thread(db.rollback)
        raise
    finally:
        await asyncio.to_thread(db.close)


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
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        request_timeout_ms=90_000,   # absorb link jitter on slow paths
    )
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