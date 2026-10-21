from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator


class AlertOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    premises_id: int
    rule_id: int
    type: str
    severity: str
    state: str
    title: str
    message: str
    current_value: Decimal | None
    threshold_value: Decimal | None
    unit: str | None
    occurrence_count: int
    first_detected_at: datetime
    last_detected_at: datetime
    acknowledged_at: datetime | None
    snoozed_until: datetime | None
    resolved_at: datetime | None


class SnoozeRequest(BaseModel):
    until: datetime

    @field_validator("until")
    @classmethod
    def _normalize_until(cls, v: datetime) -> datetime:
        # A naive datetime is interpreted as UTC; must be in the future.
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        if v <= datetime.now(timezone.utc):
            raise ValueError("until must be in the future")
        return v


class AcknowledgeAllOut(BaseModel):
    premises_id: int
    acknowledged_count: int
    alert_ids: list[int]


class NotificationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    channel: str
    status: str


class DetectionOutcomeOut(BaseModel):
    rule_type: str
    condition_key: str
    condition_met: bool
    status: str
    event_type: str | None = None
    condition_instance_id: str