import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import CommitFailedError, KafkaError

from src.config import settings
from src.database import SessionLocal
from src.detection.engine import find_matching_rules, process_reading
from src.schemas.input_message import EnergyReading

logger = logging.getLogger(__name__)

BATCH_SIZE = settings.CONSUMER_BATCH_SIZE
POLL_TIMEOUT_MS = settings.KAFKA_POLL_TIMEOUT_MS
HEARTBEAT_INTERVAL_SECONDS = settings.HEARTBEAT_INTERVAL_SECONDS


async def handle_batch(raw_values: list[bytes]) -> int:
    """
    Parse and evaluate a whole batch of readings inside ONE db transaction.
    Rules are loaded once per batch (not per reading). Returns how many
    readings were processed.
    """
    readings = []
    for raw in raw_values:
        try:
            readings.append(EnergyReading(**json.loads(raw)))
        except Exception:
            logger.exception("Skipping unparseable meter reading")
    if not readings:
        return 0

    db = SessionLocal()
    try:
        rules = find_matching_rules(db)
        for reading in readings:
            logger.debug(
                "Processing reading: premises=%s meter=%s total_kwh_period=%s",
                reading.premises_id, reading.meter_type, reading.total_kwh_period,
            )
            process_reading(db, reading, rules=rules)
        db.commit()
        return len(readings)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


async def _heartbeat(stats: dict, interval_seconds: int = HEARTBEAT_INTERVAL_SECONDS) -> None:
    """Periodic liveness line so idle/high-latency periods are visible at INFO."""
    while True:
        await asyncio.sleep(interval_seconds)
        logger.info(
            "alive: batches=%d readings=%d last_partition=%s last_offset=%s",
            stats["batches"], stats["readings"],
            stats.get("last_partition"), stats.get("last_offset"),
        )


async def run_consumer() -> None:
    """
    Long-running loop: pull batches of meter readings off Kafka and run them
    through the Detection Engine. Each batch is evaluated in a single db
    transaction; Kafka offsets are committed only after that transaction
    commits (at-least-once). Runs as its own process:

        python -m src.kafka.consumer
    """
    consumer = AIOKafkaConsumer(
        settings.METER_READINGS_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=BATCH_SIZE,
        # tolerate long GC pauses / network blips without getting kicked:
        max_poll_interval_ms=600_000,   # 10 min between polls is still fine
        session_timeout_ms=60_000,
        heartbeat_interval_ms=10_000,
        request_timeout_ms=120_000,     # must exceed session_timeout; absorbs link jitter
    )
    await consumer.start()
    logger.info("Detection Engine consumer started on topic %s (batch=%d)", settings.METER_READINGS_TOPIC, BATCH_SIZE)

    stats: dict = {"batches": 0, "readings": 0}
    heartbeat = asyncio.create_task(_heartbeat(stats))
    try:
        while True:
            results = await consumer.getmany(timeout_ms=POLL_TIMEOUT_MS, max_records=BATCH_SIZE)
            messages = [msg for messages in results.values() for msg in messages]
            if not messages:
                continue

            await handle_batch([msg.value for msg in messages])
            try:
                await consumer.commit()
            except (CommitFailedError, KafkaError):
                # group rebalanced / member kicked (e.g. network pause or a
                # second instance with the same group). Log and keep going:
                # the next poll rejoins automatically and the batch is simply
                # reprocessed (safe -- detection transitions are idempotent).
                logger.warning("Offset commit failed after rebalance; reprocessing current batch", exc_info=True)
                continue  # whole batch at once, after db commit

            last = messages[-1]
            stats["batches"] += 1
            stats["readings"] += len(messages)
            stats["last_partition"] = last.partition
            stats["last_offset"] = last.offset
    finally:
        heartbeat.cancel()
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
