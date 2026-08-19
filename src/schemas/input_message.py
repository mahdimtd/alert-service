from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, ConfigDict


class EnergyReading(BaseModel):
    model_config = ConfigDict(frozen=True)

    premises_id: int
    meter_type: str
    measured_at: datetime
    timezone: str

    total_kwh_hourly: Decimal
    total_kwh_daily: Decimal
    total_kwh_period: Decimal
    pattern_threshold_period: Decimal

    tou_bucket: str | None = None
    steps_period: list = []
    current_step: int | None = None
    next_step: int | None = None

    cost_current: Decimal
    cost_hourly: Decimal
    cost_daily: Decimal
    cost_period: Decimal