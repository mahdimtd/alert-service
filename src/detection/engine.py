import operator as op
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.detection import state as condition_state
from src.detection.metrics import METRIC_EXTRACTORS
from src.detection.outbox import enqueue_condition_event
from src.detection.thresholds import resolve_threshold
from src.models.alert_rule import AlertRule
from src.schemas.input_message import EnergyReading

_OPERATORS = {
    "GT": op.gt,
    "GTE": op.ge,
    "LT": op.lt,
    "LTE": op.le,
    "EQ": op.eq,
}


def find_matching_rules(db: Session) -> list[AlertRule]:
    stmt = select(AlertRule).where(AlertRule.enabled == True)  # noqa: E712
    return list(db.execute(stmt).scalars().all())


def rule_condition_met(operator: str, value: Decimal, threshold: Decimal) -> bool:
    return _OPERATORS[operator](value, threshold)


def _event_payload(
    rule: AlertRule,
    reading: EnergyReading,
    transition: condition_state.ConditionTransition,
    value: Decimal,
    threshold: Decimal,
) -> dict:
    row = transition.state
    return {
        "alert_rule_id": rule.id,
        "rule_type": rule.type,
        "rule_name": rule.name,
        "premises_id": row.premises_id,
        "condition_instance_id": str(row.condition_instance_id),
        "metric": rule.metric,
        "operator": rule.operator,
        "value": str(value),
        "threshold": str(threshold),
        "pattern_threshold_period": str(reading.pattern_threshold_period),
        "unit": rule.unit,
        "severity": rule.severity,
        "cooldown_minutes": rule.cooldown_minutes,
        "occurred_at": reading.measured_at.isoformat(),
        "status": row.status,
    }


def process_reading(db: Session, reading: EnergyReading, rules: list[AlertRule] | None = None) -> list[dict]:
    outcomes: list[dict] = []
    if rules is None:
        rules = find_matching_rules(db)

    for rule in rules:
        extractor = METRIC_EXTRACTORS.get(rule.metric)
        value = extractor(reading) if extractor else None
        if value is None:
            continue  # this rule has no usable data in the current reading

        threshold = resolve_threshold(rule, reading.pattern_threshold_period)
        met = rule_condition_met(rule.operator, value, threshold)

        transition = condition_state.apply_reading(
            db, rule, reading.premises_id, value, met, threshold,
        )

        if transition.transitioned:
            payload = _event_payload(rule, reading, transition, value, threshold)
            enqueue_condition_event(db, transition.state, transition.event_type, payload)

        outcomes.append({
            "rule_type": rule.type,
            "condition_met": met,
            "status": transition.state.status,
            "event_type": transition.event_type,
            "condition_instance_id": str(transition.state.condition_instance_id),
        })

    return outcomes