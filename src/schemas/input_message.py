from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

#what is the final input data for service?!
class MeterReading(BaseModel):
    model_config = ConfigDict(frozen=True)

    premises_id: int
    meter_type: str  # matches alert_rules.suitable_for entries: FAHAM, POWER_METER
    metric: str  # matches alert_rules.metric, e.g. "hourly_consumption_kwh"
    value: Decimal
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))