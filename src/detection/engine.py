import operator as op
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.detection import state as condition_state
from src.detection.outbox import enqueue_condition_event
from src.models.alert_rule import AlertRule
from src.schemas.input_message import MeterReading

# Maps the DB-level operator codes to actual comparisons.
_OPERATORS = {
    "GT": op.gt,
    "GTE": op.ge,
    "LT": op.lt,
    "LTE": op.le,
    "EQ": op.eq,
}


def find_matching_rules(db: Session, metric: str, meter_type: str) -> list[AlertRule]:
    """Enabled rules that apply to this metric and this meter type."""
    stmt = select(AlertRule).where(
        AlertRule.enabled == True,  # noqa: E712
        AlertRule.metric == metric,
        AlertRule.suitable_for.any(meter_type),
    )
    return list(db.execute(stmt).scalars().all())


def rule_condition_met(rule: AlertRule, value: Decimal) -> bool:
    """
    Whether the reading currently satisfies the rule's threshold.

    NOTE: this only compares the single incoming value against the
    threshold; it does not yet account for rule.duration_minutes (the
    condition needing to hold for a sustained period). Doing that
    properly needs a rolling window over past readings, and this project
    has no raw-readings table yet -- the metering pipeline is external.
    For now every reading is treated as an instantaneous check; revisit
    once we decide where sustained-window state should live.
    """
    comparator = _OPERATORS[rule.operator]
    return comparator(value, rule.threshold)


def _event_payload(rule: AlertRule, reading: MeterReading, transition: condition_state.ConditionTransition) -> dict:
    row = transition.state
    # Denormalized on purpose: this payload is what leaves the process
    # (via the outbox + Kafka), so it must be self-sufficient. The
    # Lifecycle Manager consuming it never joins back to alert_rules or
    # condition_states to make sense of it.
    return {
        "alert_rule_id": rule.id,
        "rule_type": rule.type,
        "rule_name": rule.name,
        "premises_id": row.premises_id,
        "condition_key": row.condition_key,
        "condition_instance_id": str(row.condition_instance_id),
        "metric": rule.metric,
        "operator": rule.operator,
        "value": str(reading.value),
        "threshold": str(rule.threshold),
        "unit": rule.unit,
        "severity": rule.severity,
        "cooldown_minutes": rule.cooldown_minutes,
        "occurred_at": reading.occurred_at.isoformat(),
        "status": row.status,
    }


def process_reading(db: Session, reading: MeterReading) -> list[dict]:
    """
    Evaluate a meter reading against every enabled rule that applies to
    it, and update this reading's local condition state in
    detection.condition_states.

    The Detection Engine does NOT call into the Lifecycle Manager
    directly anymore -- it never hands it a Python object, and never
    queries alert.alerts. Whenever a condition genuinely starts
    (INACTIVE/CLEARED -> ACTIVE) or genuinely stops (ACTIVE -> CLEARED),
    it writes a self-contained event row to detection.outbox_events in
    the SAME transaction as the state change. A separate relay process
    (src/kafka/detection_relay.py) publishes those rows to Kafka
    (settings.CONDITION_EVENTS_TOPIC); the Lifecycle Manager consumes
    that topic in its own process (src/kafka/condition_consumer.py) and
    only THEN touches alert.alerts.

    Readings that just repeat an already-ACTIVE (or already
    not-active) condition update bookkeeping only -- no event, no
    Lifecycle Manager call, no alerts-table query at all.

    Returns a list of outcome dicts, one per rule evaluated, describing
    the local condition state after this reading. This is NOT "what
    happened to any alert" -- alert creation/resolution is now an async,
    downstream concern this function doesn't know about.
    """
    outcomes: list[dict] = []

    rules = find_matching_rules(db, reading.metric, reading.meter_type)

    for rule in rules:
        met = rule_condition_met(rule, reading.value)
        transition = condition_state.apply_reading(
            db, rule, reading.premises_id, reading.value, met,
        )

        if transition.transitioned:
            payload = _event_payload(rule, reading, transition)
            enqueue_condition_event(db, transition.state, transition.event_type, payload)

        outcomes.append({
            "rule_type": rule.type,
            "condition_key": transition.state.condition_key,
            "condition_met": met,
            "status": transition.state.status,
            "event_type": transition.event_type,
            "condition_instance_id": str(transition.state.condition_instance_id),
        })

    return outcomes
