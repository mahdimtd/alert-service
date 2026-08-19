from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models.alert_audit_logs import AlertAuditLog
from src.models.alert_rule import AlertRule
from src.models.alerts import Alert
from src.outbox import enqueue as enqueue_outbox_event
from src.schemas.alert_signal import AlertSignal

# States in which an alert is still considered "open" (matches the partial
# unique index on alerts.fingerprint in the database).
OPEN_STATES = ("ACTIVE", "ACKNOWLEDGED", "SNOOZED")


class LifecycleError(Exception):
    """Raised when a requested lifecycle transition is not valid."""


def build_fingerprint(premises_id: int, rule_type: str) -> str:
    """
    Identify a single ongoing occurrence of a rule for a given premises.
    Used to decide whether a signal belongs to an existing open alert
    or should start a new one.
    """
    return f"{premises_id}:{rule_type}"


def _outbox_payload(alert: Alert, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "alert_id": alert.id,
        "premises_id": alert.premises_id,
        "rule_type": alert.type,
        "severity": alert.severity,
        "state": alert.state,
        "occurrence_count": alert.occurrence_count,
    }
    if extra:
        payload.update(extra)
    return payload


def get_open_alert(db: Session, premises_id: int, fingerprint: str) -> Alert | None:
    stmt = select(Alert).where(
        Alert.premises_id == premises_id,
        Alert.fingerprint == fingerprint,
        Alert.state.in_(OPEN_STATES),
    )
    return db.execute(stmt).scalar_one_or_none()


def _log_event(
    db: Session,
    alert_id: int,
    event: str,
    from_state: str | None,
    to_state: str | None,
    actor_type: str = "SYSTEM",
    metadata: dict[str, Any] | None = None,
) -> AlertAuditLog:
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


def _default_title_and_message(rule: AlertRule, value: Decimal, threshold: Decimal) -> tuple[str, str]:
    title = rule.name
    message = (
        f"{rule.metric} is {value} {rule.unit}, "
        f"crossing the {rule.operator} {threshold} {rule.unit} threshold."
    )
    return title, message


def process_signal(db: Session, signal: AlertSignal) -> tuple[Alert, bool]:
    rule = db.get(AlertRule, signal.rule_id)
    if rule is None:
        raise LifecycleError(f"Alert rule {signal.rule_id} not found")

    fingerprint = build_fingerprint(signal.premises_id, rule.type)
    now = datetime.now(timezone.utc)

    existing = get_open_alert(db, signal.premises_id, fingerprint)

    if existing is None:
        title, message = _default_title_and_message(rule, signal.value, signal.threshold)
        alert = Alert(
            premises_id=signal.premises_id,
            rule_id=rule.id,
            type=rule.type,
            severity=rule.severity,
            state="ACTIVE",
            title=title,
            message=message,
            current_value=signal.value,
            threshold_value=signal.threshold,   # was rule.threshold
            unit=rule.unit,
            fingerprint=fingerprint,
        )
        db.add(alert)
        db.flush()
        _log_event(db, alert.id, "ALERT_CREATED", from_state=None, to_state="ACTIVE")
        enqueue_outbox_event(db, "ALERT", alert.id, "alert.created", _outbox_payload(alert))
        return alert, True

    previous_state = existing.state
    was_snoozed = previous_state == "SNOOZED"

    within_cooldown = (
        now - existing.last_detected_at
    ).total_seconds() < rule.cooldown_minutes * 60

    existing.occurrence_count += 1
    existing.last_detected_at = now
    existing.current_value = signal.value
    existing.threshold_value = signal.threshold  # NEW: threshold can drift per-reading now

    # ... rest unchanged

def resolve_alert(db: Session, premises_id: int, rule_type: str) -> Alert | None:
    """
    Mark the open alert for this premises/rule as resolved because the
    underlying condition is no longer met. Returns None if there was no
    open alert to resolve (e.g. Detection Engine reports "back to normal"
    for something that was never alerted on).
    """
    fingerprint = build_fingerprint(premises_id, rule_type)
    alert = get_open_alert(db, premises_id, fingerprint)
    if alert is None:
        return None

    previous_state = alert.state
    alert.state = "RESOLVED"
    alert.resolved_at = datetime.now(timezone.utc)

    _log_event(
        db, alert.id, "ALERT_RESOLVED",
        from_state=previous_state, to_state="RESOLVED",
    )
    enqueue_outbox_event(
        db, "ALERT", alert.id, "alert.resolved",
        _outbox_payload(alert, {"from_state": previous_state}),
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

    _log_event(
        db, alert.id, "ALERT_ACKNOWLEDGED",
        from_state=previous_state, to_state="ACKNOWLEDGED", actor_type=actor_type,
    )
    enqueue_outbox_event(
        db, "ALERT", alert.id, "alert.acknowledged",
        _outbox_payload(alert, {"from_state": previous_state}),
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

    _log_event(
        db, alert.id, "ALERT_SNOOZED",
        from_state=previous_state, to_state="SNOOZED", actor_type=actor_type,
    )
    enqueue_outbox_event(
        db, "ALERT", alert.id, "alert.snoozed",
        _outbox_payload(alert, {"from_state": previous_state, "snoozed_until": until.isoformat()}),
    )
    return alert