from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.detection.identifiers import rule_uuid
from src.models.alert_rule import AlertRule
from src.models.condition_states import ConditionState

# Every rule currently represents a single, instantaneous "value crossed
# threshold" condition, so one condition_key is enough today. If a rule
# ever needs more than one independently-tracked condition (e.g. an
# instantaneous breach AND a separate sustained-duration breach), give
# each its own condition_key instead of overloading this one.
THRESHOLD_BREACH_CONDITION_KEY = "threshold_breach"

# Placeholder until alert_rules gains a real version column. Every
# condition_states row is written with this fixed value for now. The
# column itself is fine as-is -- it's meant to track the version of the
# rule's *content*, not of this row -- it just has nothing real to point
# at yet.
RULE_CONTENT_VERSION_PLACEHOLDER = 1

ACTIVE = "ACTIVE"
INACTIVE = "INACTIVE"
CLEARED = "CLEARED"

CONDITION_DETECTED = "ConditionDetected"
CONDITION_CLEARED = "ConditionCleared"


@dataclass
class ConditionTransition:
    state: ConditionState
    event_type: str | None  # ConditionDetected / ConditionCleared / None (no transition)

    @property
    def transitioned(self) -> bool:
        return self.event_type is not None


def _get_locked(
    db: Session, premises_id: int, rule_id, condition_key: str
) -> ConditionState | None:
    # Row-level lock so two readings for the same (premises, rule,
    # condition_key) landing in overlapping transactions can't both
    # decide "this is a fresh transition" at once.
    stmt = (
        select(ConditionState)
        .where(
            ConditionState.premises_id == premises_id,
            ConditionState.rule_id == rule_id,
            ConditionState.condition_key == condition_key,
        )
        .with_for_update()
    )
    return db.execute(stmt).scalar_one_or_none()


def apply_reading(
    db: Session,
    rule: AlertRule,
    premises_id: int,
    value: Decimal,
    condition_met: bool,
    threshold: Decimal,
    condition_key: str = THRESHOLD_BREACH_CONDITION_KEY,
) -> ConditionTransition:
    rid = rule_uuid(rule.id)
    now = datetime.now(timezone.utc)

    row = _get_locked(db, premises_id, rid, condition_key)

    if row is None:
        row = ConditionState(
            premises_id=premises_id,
            rule_id=rid,
            condition_key=condition_key,
            status=INACTIVE,
            rule_version=RULE_CONTENT_VERSION_PLACEHOLDER,
        )
        db.add(row)

    was_active = row.status == ACTIVE
    event_type: str | None = None

    row.last_observed_at = now
    row.last_value = value
    row.threshold_value = threshold  # was rule.threshold
    row.version = (row.version or 0) + 1

    if condition_met and not was_active:
        row.status = ACTIVE
        row.condition_instance_id = uuid4()
        row.first_detected_at = now
        row.cleared_at = None
        event_type = CONDITION_DETECTED
    elif not condition_met and was_active:
        row.status = CLEARED
        row.cleared_at = now
        event_type = CONDITION_CLEARED

    db.flush()
    return ConditionTransition(state=row, event_type=event_type)