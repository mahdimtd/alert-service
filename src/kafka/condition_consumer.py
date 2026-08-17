import asyncio
import json
import logging
from decimal import Decimal

from aiokafka import AIOKafkaConsumer

from src.config import settings
from src.database import SessionLocal
from src.detection.state import CONDITION_CLEARED, CONDITION_DETECTED
from src.lifecycle import manager as lifecycle
from src.orchestrator.superviser import create_notifications
from src.schemas.alert_signal import AlertSignal

logger = logging.getLogger(__name__)


async def handle_message(raw_value: bytes) -> None:
    try:
        message = json.loads(raw_value)
        event_type = message["event_type"]
        payload = message["payload"]
    except Exception:
        logger.exception("Skipping unparseable condition event message")
        return

    db = SessionLocal()
    try:
        if event_type == CONDITION_DETECTED:
            signal = AlertSignal(
                premises_id=payload["premises_id"],
                rule_id=payload["alert_rule_id"],
                value=Decimal(payload["value"]),
            )
            alert, should_notify = lifecycle.process_signal(db, signal)
            if should_notify:
                create_notifications(db, alert)

        elif event_type == CONDITION_CLEARED:
            lifecycle.resolve_alert(db, payload["premises_id"], payload["rule_type"])

        else:
            logger.warning("Ignoring unknown condition event type: %s", event_type)

        db.commit()
    except lifecycle.LifecycleError:
        db.rollback()
        logger.exception("Lifecycle error processing condition event: %s", message)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


async def run_consumer() -> None:
    """
    Long-running loop: read ConditionDetected/ConditionCleared events off
    Kafka (produced by src/kafka/detection_relay.py from
    detection.outbox_events) and drive the Lifecycle Manager, then the
    Notification Orchestrator, from them. Runs as its own process:

        python -m src.kafka.condition_consumer

    This is the ONLY place that turns a detected condition into an
    alert.alerts row. The Detection Engine never calls the Lifecycle
    Manager directly and never touches alert.alerts -- it only produces
    the events this process consumes.

    NOTE on delivery semantics: same at-least-once caveat as
    src/kafka/consumer.py. A replayed ConditionDetected calls
    process_signal again (counted as another occurrence, same behavior
    as the old in-process call). A replayed ConditionCleared calls
    resolve_alert again, which is a safe no-op if the alert is already
    RESOLVED.
    """
    consumer = AIOKafkaConsumer(
        settings.CONDITION_EVENTS_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.LIFECYCLE_KAFKA_CONSUMER_GROUP,
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
