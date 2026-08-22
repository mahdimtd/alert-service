import asyncio
import json
import logging
from decimal import Decimal

from aiokafka import AIOKafkaConsumer

from src.config import settings
from src.database import SessionLocal
from src.detection.state import CONDITION_CLEARED, CONDITION_DETECTED
from src.lifecycle import manager as lifecycle
from src.schemas.alert_signal import AlertSignal

logger = logging.getLogger(__name__)

BATCH_SIZE = settings.LIFECYCLE_BATCH_SIZE
POLL_TIMEOUT_MS = settings.KAFKA_POLL_TIMEOUT_MS
HEARTBEAT_INTERVAL_SECONDS = settings.HEARTBEAT_INTERVAL_SECONDS


async def handle_batch(raw_values: list[bytes]) -> int:
    """
    Process a batch of ConditionDetected/ConditionCleared events in ONE db
    transaction. Each event runs inside its own SAVEPOINT, so one bad event
    (e.g. unknown rule) rolls back only itself and the rest of the batch
    still commits. Returns how many events were processed successfully.
    """
    events = []
    for raw in raw_values:
        try:
            message = json.loads(raw)
            events.append((message["event_type"], message["payload"]))
        except Exception:
            logger.exception("Skipping unparseable condition event message")
    if not events:
        return 0

    db = SessionLocal()
    processed = 0
    try:
        for event_type, payload in events:
            try:
                with db.begin_nested():  # SAVEPOINT: isolate this event
                    if event_type == CONDITION_DETECTED:
                        signal = AlertSignal(
                            premises_id=payload["premises_id"],
                            rule_id=payload["alert_rule_id"],
                            value=Decimal(payload["value"]),
                            threshold=Decimal(payload["threshold"]),
                        )
                        # process_signal enqueues notification.requested itself
                        # (same transaction) whenever policy says notify.
                        lifecycle.process_signal(db, signal)
                    elif event_type == CONDITION_CLEARED:
                        lifecycle.resolve_alert(db, payload["premises_id"], payload["rule_type"])
                    else:
                        logger.warning("Ignoring unknown condition event type: %s", event_type)
                processed += 1
            except lifecycle.LifecycleError:
                logger.exception("Lifecycle error processing condition event (type=%s)", event_type)
        db.commit()
        return processed
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
            "alive: batches=%d events=%d last_partition=%s last_offset=%s",
            stats["batches"], stats["events"],
            stats.get("last_partition"), stats.get("last_offset"),
        )


async def run_consumer() -> None:
    """
    Long-running loop: pull batches of condition events off Kafka and drive
    the Lifecycle Manager from them. Runs as its own process:

        python -m src.kafka.condition_consumer

    Offsets are committed only after the batch's db transaction commits
    (at-least-once). Replays are safe: process_signal counts another
    occurrence and resolve_alert is a no-op on already-RESOLVED alerts.
    """
    consumer = AIOKafkaConsumer(
        settings.CONDITION_EVENTS_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.LIFECYCLE_KAFKA_CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=BATCH_SIZE,
    )
    await consumer.start()
    logger.info("Lifecycle Manager consumer started on topic %s (batch=%d)", settings.CONDITION_EVENTS_TOPIC, BATCH_SIZE)

    stats: dict = {"batches": 0, "events": 0}
    heartbeat = asyncio.create_task(_heartbeat(stats))
    try:
        while True:
            results = await consumer.getmany(timeout_ms=POLL_TIMEOUT_MS, max_records=BATCH_SIZE)
            messages = [msg for messages in results.values() for msg in messages]
            if not messages:
                continue

            await handle_batch([msg.value for msg in messages])
            await consumer.commit()

            last = messages[-1]
            stats["batches"] += 1
            stats["events"] += len(messages)
            stats["last_partition"] = last.partition
            stats["last_offset"] = last.offset
    finally:
        heartbeat.cancel()
        await consumer.stop()
        logger.info("Lifecycle Manager consumer stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s'
    )
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logger.setLevel(logging.DEBUG)
    asyncio.run(run_consumer())
