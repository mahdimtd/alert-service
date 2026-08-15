from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


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


class NotificationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    channel: str
    status: str


class ReadingResultOut(BaseModel):
    rule_type: str
    condition_met: bool
    alert_id: int
    state: str
    should_notify: bool | None = None
    notifications: list[NotificationOut] = []