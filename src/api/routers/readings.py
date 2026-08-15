from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.api.schemas import ReadingResultOut
from src.database import get_db
from src.detection.engine import process_reading
from src.lifecycle.manager import LifecycleError
from src.models.alerts import Alert
from src.orchestrator.superviser import create_notifications
from src.schemas.input_message import MeterReading

router = APIRouter(prefix="/readings", tags=["readings"])


@router.post("", response_model=list[ReadingResultOut])
def submit_reading(reading: MeterReading, db: Session = Depends(get_db)):
    """
    Feed one meter reading through the full pipeline: Detection Engine
    evaluates matching rules, Lifecycle Manager creates/updates/resolves
    alerts, and the Orchestrator queues notifications for anything that
    needs one right now. Mainly meant for manual testing/integration --
    real ingestion will eventually come off Kafka instead of this endpoint.
    """
    try:
        outcomes = process_reading(db, reading)
    except LifecycleError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

    results = []
    for outcome in outcomes:
        notifications = []
        if outcome.get("condition_met") and outcome.get("should_notify"):
            alert = db.get(Alert, outcome["alert_id"])
            notifications = create_notifications(db, alert)
        results.append({**outcome, "notifications": notifications})

    db.commit()
    return results