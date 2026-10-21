from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.schemas import AcknowledgeAllOut, AlertOut, SnoozeRequest
from src.database import get_db
from src.lifecycle import manager as lifecycle

router = APIRouter(prefix="/alerts", tags=["alerts"])


def _raise_http(exc: lifecycle.LifecycleError) -> None:
    if isinstance(exc, lifecycle.AlertNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, lifecycle.InvalidStateError):
        raise HTTPException(status_code=409, detail=str(exc))
    raise HTTPException(status_code=400, detail=str(exc))


@router.post("/acknowledge-all", response_model=AcknowledgeAllOut)
def acknowledge_all(
    premises_id: int = Query(...),
    db: Session = Depends(get_db),
):
    """Mark every ACTIVE/SNOOZED alert of the premises as acknowledged ("read all")."""
    try:
        alerts = lifecycle.acknowledge_all_alerts(db, premises_id, actor_type="USER")
        db.commit()
    except lifecycle.LifecycleError as e:
        db.rollback()
        _raise_http(e)
    except Exception:
        db.rollback()
        raise
    return AcknowledgeAllOut(
        premises_id=premises_id,
        acknowledged_count=len(alerts),
        alert_ids=[a.id for a in alerts],
    )


@router.post("/{alert_id}/acknowledge", response_model=AlertOut)
def acknowledge_alert(alert_id: int, db: Session = Depends(get_db)):
    try:
        alert = lifecycle.acknowledge_alert(db, alert_id, actor_type="USER")
        db.commit()
    except lifecycle.LifecycleError as e:
        db.rollback()
        _raise_http(e)
    except Exception:
        db.rollback()
        raise
    db.refresh(alert)
    return alert


@router.post("/{alert_id}/snooze", response_model=AlertOut)
def snooze_alert(alert_id: int, body: SnoozeRequest, db: Session = Depends(get_db)):
    try:
        alert = lifecycle.snooze_alert(db, alert_id, until=body.until, actor_type="USER")
        db.commit()
    except lifecycle.LifecycleError as e:
        db.rollback()
        _raise_http(e)
    except Exception:
        db.rollback()
        raise
    db.refresh(alert)
    return alert