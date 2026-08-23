from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.models.alert_rule import AlertRule
from src.models.condition_states import ConditionState


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


def _get_locked(db: Session, premises_id: int, rule_id) -> ConditionState:
    # Row-level lock so two readings for the same (premises, rule,
    # condition_key) landing in overlapping transactions can't both
    # decide "this is a fresh transition" at once.
    #
    # NOTE: `SELECT ... FOR UPDATE` can only lock a row that already
    # exists. If two consumer processes both see the FIRST-EVER reading
    # for the same (premises_id, rule_id) at the same time, both will
    # find nothing to lock, both will try to INSERT, and one will fail
    # with a duplicate-key error on the (premises_id, rule_id) primary
    # key. The upsert below makes row creation itself atomic (the second
    # writer's INSERT is a no-op instead of a conflict), so by the time
    # we run the SELECT ... FOR UPDATE, the row is guaranteed to exist
    # and exactly one process will hold the lock at a time.
    insert_stmt = (
        pg_insert(ConditionState)
        .values(
            premises_id=premises_id,
            rule_id=rule_id,
            status=INACTIVE,
            rule_version=RULE_CONTENT_VERSION_PLACEHOLDER,
            # Explicit 0 (instead of relying on the column's server_default
            # of 1) so the "+1" below still lands on version=1 for a
            # brand-new row, matching the previous behavior.
            version=0,
        )
        .on_conflict_do_nothing(
            index_elements=[ConditionState.premises_id, ConditionState.rule_id],
        )
    )
    db.execute(insert_stmt)

    stmt = (
        select(ConditionState)
        .where(
            ConditionState.premises_id == premises_id,
            ConditionState.rule_id == rule_id,
        )
        .with_for_update()
    )
    return db.execute(stmt).scalar_one()


def apply_reading(
    db: Session,
    rule: AlertRule,
    premises_id: int,
    value: Decimal,
    condition_met: bool,
    threshold: Decimal,
) -> ConditionTransition:
    rid = rule.id
    now = datetime.now(timezone.utc)

    # _get_locked always returns an existing, locked row now (it upserts
    # first), so there's no separate "create if missing" branch here.
    row = _get_locked(db, premises_id, rid)

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