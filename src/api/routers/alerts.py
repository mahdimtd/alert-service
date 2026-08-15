from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.api.schemas import AlertOut, SnoozeRequest
from src.database import get_db
from src.lifecycle import manager as lifecycle
from src.models.alerts import Alert

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=list[AlertOut])
def list_alerts(
    premises_id: int | None = None,
    state: str | None = None,
    db: Session = Depends(get_db),
):
    stmt = select(Alert).order_by(Alert.created_at.desc())
    if premises_id is not None:
        stmt = stmt.where(Alert.premises_id == premises_id)
    if state is not None:
        stmt = stmt.where(Alert.state == state)
    return db.execute(stmt).scalars().all()


@router.get("/{alert_id}", response_model=AlertOut)
def get_alert(alert_id: int, db: Session = Depends(get_db)):
    alert = db.get(Alert, alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert


@router.post("/{alert_id}/acknowledge", response_model=AlertOut)
def acknowledge_alert(alert_id: int, db: Session = Depends(get_db)):
    try:
        alert = lifecycle.acknowledge_alert(db, alert_id, actor_type="USER")
        db.commit()
        db.refresh(alert)
        return alert
    except lifecycle.LifecycleError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{alert_id}/snooze", response_model=AlertOut)
def snooze_alert(alert_id: int, body: SnoozeRequest, db: Session = Depends(get_db)):
    try:
        alert = lifecycle.snooze_alert(db, alert_id, until=body.until, actor_type="USER")
        db.commit()
        db.refresh(alert)
        return alert
    except lifecycle.LifecycleError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))