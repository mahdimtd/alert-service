import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from src.config import settings
from src.database import SessionLocal
from src.detection.engine import process_reading
from src.schemas.input_message import EnergyReading

logger = logging.getLogger(__name__)


async def handle_message(raw_value: bytes) -> None:
    try:
        data = json.loads(raw_value)
        reading = EnergyReading(**data)
    except Exception:
        logger.exception("Skipping unparseable meter reading message")
        return
    logger.debug("Processing reading: premises=%s meter=%s value=%s", reading.premises_id, reading.meter_id, reading.value)

    db = SessionLocal()
    try:
        process_reading(db, reading)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


async def run_consumer() -> None:
    """
    Long-running loop: read meter readings off Kafka and run them
    through the Detection Engine only. Runs as its own process:

        python -m src.kafka.consumer

    This process never touches the Lifecycle Manager or alert.alerts --
    it only decides, per rule, whether a condition just started or just
    stopped, and records that as a row in detection.outbox_events. See
    src/kafka/detection_relay.py (publishes those rows to Kafka) and
    src/kafka/condition_consumer.py (turns them into alerts) for the
    rest of the pipeline.

    NOTE on delivery semantics: offsets are committed only after a
    message is fully processed and its db transaction is committed
    (enable_auto_commit=False). That gives at-least-once delivery -- if
    the process crashes after the db commit but before the Kafka offset
    commit, the same reading is replayed. process_reading has no
    message-level idempotency key, so a replayed reading is evaluated
    again; since only genuine INACTIVE/CLEARED -> ACTIVE or
    ACTIVE -> CLEARED transitions produce an outbox event, a replay that
    lands on an already-ACTIVE (or already not-active) condition is a
    harmless no-op event-wise. Add a message-level idempotency key if
    this turns out to matter for the bookkeeping columns too.
    """
    consumer = AIOKafkaConsumer(
        settings.METER_READINGS_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("Detection Engine consumer started on topic %s", settings.METER_READINGS_TOPIC)
    try:
        async for msg in consumer:
            logger.debug("Received message from partition %d offset %d", msg.partition, msg.offset)
            await handle_message(msg.value)
            await consumer.commit()
    finally:
        await consumer.stop()
        logger.info("Detection Engine consumer stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s'
    )
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logger.setLevel(logging.DEBUG)
    asyncio.run(run_consumer())
