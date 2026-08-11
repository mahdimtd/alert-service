import operator as op
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.lifecycle import manager as lifecycle
from src.models.alert_rule import AlertRule
from src.schemas.alert_signal import AlertSignal
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


def process_reading(db: Session, reading: MeterReading) -> list[dict]:
    """
    Evaluate a meter reading against every enabled rule that applies to
    it, and hand each result to the Lifecycle Manager:
      - condition met      -> lifecycle.process_signal (create/update alert)
      - condition not met  -> lifecycle.resolve_alert (close it if it was open)

    Returns a list of outcome dicts, one per rule that was evaluated and
    actually resulted in a lifecycle change (a resolve with no open alert
    produces no outcome).
    """
    outcomes: list[dict] = []

    rules = find_matching_rules(db, reading.metric, reading.meter_type)

    for rule in rules:
        if rule_condition_met(rule, reading.value):
            signal = AlertSignal(
                premises_id=reading.premises_id,
                rule_id=rule.id,
                value=reading.value,
            )
            alert, should_notify = lifecycle.process_signal(db, signal)
            outcomes.append({
                "rule_type": rule.type,
                "condition_met": True,
                "alert_id": alert.id,
                "state": alert.state,
                "should_notify": should_notify,
            })
        else:
            resolved = lifecycle.resolve_alert(db, reading.premises_id, rule.type)
            if resolved is not None:
                outcomes.append({
                    "rule_type": rule.type,
                    "condition_met": False,
                    "alert_id": resolved.id,
                    "state": resolved.state,
                })

    return outcomes