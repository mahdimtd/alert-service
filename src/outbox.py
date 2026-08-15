from typing import Any

from sqlalchemy.orm import Session

from src.models.outbox_events import OutboxEvent


def enqueue(
    db: Session,
    aggregate_type: str,
    aggregate_id: int,
    event_type: str,
    payload: dict[str, Any],
) -> OutboxEvent:
    """
    Add a domain event to the outbox. Caller must be inside the same
    db session/transaction as the business change this event describes,
    and must still commit() -- this function only adds, it never commits.
    """
    event = OutboxEvent(
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        event_type=event_type,
        payload=payload,
    )
    db.add(event)
    return event
