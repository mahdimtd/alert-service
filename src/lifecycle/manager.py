from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models.alert_audit_logs import AlertAuditLog
from src.models.alert_rule import AlertRule
from src.models.alerts import Alert
from src.schemas.alert_signal import AlertSignal

#har alert dar database yeki iz ina hast.ager active,acknowleded bashe nmish jadid sakht.
OPEN_STATES = ("ACTIVE", "ACKNOWLEDGED", "SNOOZED")

#bejaye class toy har def ye exception bezrim.
class LifecycleError(Exception):

    """inheritance from exception for specific lifecycle errors"""


def build_fingerprint(premises_id: int, rule_type: str) -> str:
    """
    building a value for identifying ongoing alert for a given premises_id
    this is going to use for identify is this alert exist or what?!
    """
    return f"{premises_id}:{rule_type}"


def check_alert_db(db: Session, premises_id: int, fingerprint: str) -> Alert | None:
    """
    using for check if alert exists in db or what?!
    """
    stmt = select(Alert).where(
        Alert.premises_id == premises_id,
        Alert.fingerprint == fingerprint,
        Alert.state.in_(OPEN_STATES),
    )
    return db.execute(stmt).scalar_one_or_none()


def _audit_log(
    db: Session,
    alert_id: int,
    event: str,
    from_state: str | None,
    to_state: str | None,
    actor_type: str = "SYSTEM",
    metadata: dict[str, Any] | None = None,
    ) -> AlertAuditLog:
    """Create an audit log entry for db table"""

    log = AlertAuditLog(
        alert_id=alert_id,
        event=event,
        from_state=from_state,
        to_state=to_state,
        actor_type=actor_type,
        event_metadata=metadata or {},
    )
    db.add(log)
    return log


def _default_title_and_message(rule: AlertRule, value: Decimal) -> tuple[str, str]:
    """
    this def output a message for alert ab for notification orchestrator.
    """
    title = rule.name
    message = (
        f"{rule.metric} is {value} {rule.unit}, "
        f"crossing the {rule.operator} {rule.threshold} {rule.unit} threshold."
    )
    return title, message


def process_signal(db: Session, signal: AlertSignal) -> tuple[Alert, bool]:
    """
    Consume a detection signal and decide what to do with it:
      - no open alert for this fingerprint  -> create a new one (ACTIVE)
      - open alert exists and is SNOOZED    -> reactivate it (back to ACTIVE)
      - open alert exists (ACTIVE/ACKED)    -> bump occurrence count

    Returns (alert, should_notify). should_notify is False when an
    existing alert was updated while still inside its rule's cooldown
    window, so the caller (Notification Orchestrator, later) knows not
    to re-notify the user for every single occurrence.
    """
    rule = db.get(AlertRule, signal.rule_id)
    if rule is None:
        raise LifecycleError(f"Alert rule {signal.rule_id} not found")

    fingerprint = build_fingerprint(signal.premises_id, rule.type)
    now = datetime.now(timezone.utc)

    existing = check_alert_db(db, signal.premises_id, fingerprint)

    if existing is None:
        title, message = _default_title_and_message(rule, signal.value)
        alert = Alert(
            premises_id=signal.premises_id,
            rule_id=rule.id,
            type=rule.type,
            severity=rule.severity,
            state="ACTIVE",
            title=title,
            message=message,
            current_value=signal.value,
            threshold_value=rule.threshold,
            unit=rule.unit,
            fingerprint=fingerprint,
        )
        db.add(alert)
        db.flush()  # populate alert.id (Identity column) before logging
        _audit_log(db, alert.id, "ALERT_CREATED", from_state=None, to_state="ACTIVE")
        return alert, True

    previous_state = existing.state
    was_snoozed = previous_state == "SNOOZED"

    within_cooldown = (
        now - existing.last_detected_at
    ).total_seconds() < rule.cooldown_minutes * 60

    existing.occurrence_count += 1
    existing.last_detected_at = now
    existing.current_value = signal.value

    if was_snoozed:
        existing.state = "ACTIVE"
        existing.snoozed_until = None
        _audit_log(
            db, existing.id, "ALERT_REACTIVATED",
            from_state=previous_state, to_state="ACTIVE",
        )
    else:
        _audit_log(
            db, existing.id, "ALERT_UPDATED",
            from_state=previous_state, to_state=previous_state,
        )

    should_notify = was_snoozed or not within_cooldown
    return existing, should_notify


def resolve_alert(db: Session, premises_id: int, rule_type: str) -> Alert | None:
    """
    Mark the open alert for this premises/rule as resolved because the
    underlying condition is no longer met. Returns None if there was no
    open alert to resolve (e.g. Detection Engine reports "back to normal"
    for something that was never alerted on).
    """
    fingerprint = build_fingerprint(premises_id, rule_type)
    alert = check_alert_db(db, premises_id, fingerprint)
    if alert is None:
        return None

    previous_state = alert.state
    alert.state = "RESOLVED"
    alert.resolved_at = datetime.now(timezone.utc)

    _audit_log(
        db, alert.id, "ALERT_RESOLVED",
        from_state=previous_state, to_state="RESOLVED",
    )
    return alert


def acknowledge_alert(db: Session, alert_id: int, actor_type: str = "USER") -> Alert:
    """User has seen the alert. Valid from ACTIVE or SNOOZED."""
    alert = db.get(Alert, alert_id)
    if alert is None:
        raise LifecycleError(f"Alert {alert_id} not found")
    if alert.state not in ("ACTIVE", "SNOOZED"):
        raise LifecycleError(
            f"Alert {alert_id} cannot be acknowledged from state {alert.state}"
        )

    previous_state = alert.state
    alert.state = "ACKNOWLEDGED"
    alert.acknowledged_at = datetime.now(timezone.utc)
    alert.snoozed_until = None

    _audit_log(
        db, alert.id, "ALERT_ACKNOWLEDGED",
        from_state=previous_state, to_state="ACKNOWLEDGED", actor_type=actor_type,
    )
    return alert


def snooze_alert(
    db: Session, alert_id: int, until: datetime, actor_type: str = "USER"
) -> Alert:
    """User wants to silence the alert until a given time. Valid from ACTIVE or ACKNOWLEDGED."""
    alert = db.get(Alert, alert_id)
    if alert is None:
        raise LifecycleError(f"Alert {alert_id} not found")
    if alert.state not in ("ACTIVE", "ACKNOWLEDGED"):
        raise LifecycleError(
            f"Alert {alert_id} cannot be snoozed from state {alert.state}"
        )

    previous_state = alert.state
    alert.state = "SNOOZED"
    alert.snoozed_until = until

    _audit_log(
        db, alert.id, "ALERT_SNOOZED",
        from_state=previous_state, to_state="SNOOZED", actor_type=actor_type,
    )
    return alert