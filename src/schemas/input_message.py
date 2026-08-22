from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, ConfigDict, field_validator


class TariffStep(BaseModel):
    model_config = ConfigDict(frozen=True)

    step_from: Decimal
    step_to: Decimal | None = None
    price_coefficient: Decimal | None = None
    unit_price: Decimal | None = None


class EnergyReading(BaseModel):
    model_config = ConfigDict(frozen=True)

    premises_id: int
    meter_type: str
    measured_at: datetime

    @field_validator("meter_type", mode="before")
    @classmethod
    def _clean_meter_type(cls, v):
        # upstream sometimes double-encodes, e.g. '"METER"' -- normalize
        if isinstance(v, str):
            return v.strip().strip('"').strip("'").strip()
        return v

    total_kwh_hourly: Decimal
    total_kwh_daily: Decimal
    total_kwh_period: Decimal
    pattern_threshold_period: Decimal

    tou_bucket: str | None = None
    steps_period: list[TariffStep] = []
    current_step: Decimal | None = None
    next_step: Decimal | None = None

    cost_period: Decimal

    # upstream-only extras; not consumed by any rule today
    total_kwh_current: Decimal | None = None
    avg30_period: Decimal | None = None
    pattern_status_period: int | None = None
