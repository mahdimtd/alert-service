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

    event = DetectionOutboxEvent(
        aggregate_type="ConditionState",
        aggregate_id=state.condition_instance_id,
        premises_id=state.premises_id,
        rule_id=state.rule_id,
        event_type=event_type,
        topic=settings.CONDITION_EVENTS_TOPIC,
        partition_key=str(state.premises_id),
        payload=payload,
    )
    db.add(event)
    return event
