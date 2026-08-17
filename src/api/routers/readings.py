from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.api.schemas import DetectionOutcomeOut
from src.database import get_db
from src.detection.engine import process_reading
from src.schemas.input_message import MeterReading

router = APIRouter(prefix="/readings", tags=["readings"])


@router.post("", response_model=list[DetectionOutcomeOut])
def submit_reading(reading: MeterReading, db: Session = Depends(get_db)):
    """
    Feed one meter reading through the Detection Engine only, and commit
    whatever local condition-state change resulted from it (and, if a
    condition just started or stopped, the outbox event that goes with
    it).

    This endpoint no longer creates or updates alert.alerts rows or
    queues notifications -- that now happens asynchronously, the same
    way it would from the real Kafka pipeline, once
    src/kafka/detection_relay.py and src/kafka/condition_consumer.py are
    running alongside it. It's mainly meant for manually exercising the
    Detection Engine in isolation; real ingestion comes off Kafka via
    src/kafka/consumer.py instead of this endpoint.
    """
    outcomes = process_reading(db, reading)
    db.commit()
    return outcomes
