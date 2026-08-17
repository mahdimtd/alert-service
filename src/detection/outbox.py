"""
Transactional outbox for the Detection domain. Mirrors src/outbox.py
(the Alert domain's outbox) but writes into detection.outbox_events and
fills in the columns that table has (topic, partition_key,
condition_key, ...) so the relay can publish rows to Kafka without
joining back to condition_states or alert_rules.
"""
from typing import Any

from sqlalchemy.orm import Session

from src.config import settings
from src.models.condition_states import ConditionState
from src.models.detection_outbox_events import DetectionOutboxEvent


def enqueue_condition_event(
    db: Session,
    state: ConditionState,
    event_type: str,
    payload: dict[str, Any],
) -> DetectionOutboxEvent:
    """
    Record a ConditionDetected/ConditionCleared event in the SAME
    transaction as the condition_states change that caused it. Caller
    must still commit() -- this only adds the row, it never commits.
    """
    event = DetectionOutboxEvent(
        aggregate_type="ConditionState",
        aggregate_id=state.condition_instance_id,
        premises_id=state.premises_id,
        rule_id=state.rule_id,
        condition_key=state.condition_key,
        event_type=event_type,
        topic=settings.CONDITION_EVENTS_TOPIC,
        partition_key=str(state.premises_id),
        payload=payload,
    )
    db.add(event)
    return event
