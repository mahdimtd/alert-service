import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from src.config import settings
from src.database import SessionLocal
from src.detection.engine import process_reading
from src.lifecycle.manager import LifecycleError
from src.models.alerts import Alert
from src.orchestrator.superviser import create_notifications
from src.schemas.input_message import MeterReading

logger = logging.getLogger(__name__)


async def handle_message(raw_value: bytes) -> None:
    try:
        data = json.loads(raw_value)
        reading = MeterReading(**data)
    except Exception:
        # Poison message: log and move on. No dead-letter topic yet --
        # worth adding if bad messages turn out to be a real problem.
        logger.exception("Skipping unparseable meter reading message")
        return

    db = SessionLocal()
    try:
        outcomes = process_reading(db, reading)
        for outcome in outcomes:
            if outcome.get("condition_met") and outcome.get("should_notify"):
                alert = db.get(Alert, outcome["alert_id"])
                create_notifications(db, alert)
        db.commit()
    except LifecycleError:
        db.rollback()
        logger.exception("Lifecycle error processing reading: %s", reading)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


async def run_consumer() -> None:
    """
    Long-running loop: read meter readings off Kafka and push them
    through Detection -> Lifecycle -> Orchestrator. Runs as its own
    process, separate from the FastAPI app:

        python -m src.kafka.consumer

    NOTE on delivery semantics: offsets are committed only after a
    message is fully processed and its db transaction is committed
    (enable_auto_commit=False). That gives at-least-once delivery -- if
    the process crashes after the db commit but before the Kafka offset
    commit, the same reading is replayed. Nothing in process_reading /
    process_signal currently de-duplicates a replayed reading (there's
    no idempotency key on the message itself); it would just be counted
    as another occurrence, which is a bit off but not wrong in a
    dangerous way. Add a message-level idempotency key if this turns
    out to matter.
    """
    consumer = AIOKafkaConsumer(
        settings.METER_READINGS_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await handle_message(msg.value)
            await consumer.commit()
    finally:
        await consumer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_consumer())