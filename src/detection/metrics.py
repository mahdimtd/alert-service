from decimal import Decimal
from src.schemas.input_message import EnergyReading


def _peak_bucket_hourly_value(reading: EnergyReading) -> Decimal | None:
    if reading.tou_bucket != "peak":
        return None
    return reading.total_kwh_hourly



def _seasonal_pattern_percent(reading: EnergyReading) -> Decimal | None:
    if reading.next_step is None or not reading.steps_period:
        return None
    try:
        next_step_kwh = Decimal(str(reading.steps_period[reading.next_step]))
    except (IndexError,TypeError,ValueError):
        return None
    if next_step_kwh <= 0:
        return None
    return (reading.total_kwh_period/next_step_kwh) * Decimal(100)



METRIC_EXTRACTORS = {
    "daily_consumption_kwh": lambda r: r.total_kwh_daily,
    "monthly_consumption_kwh": lambda r: r.total_kwh_period,
    "hourly_consumption_kwh": lambda r: r.total_kwh_hourly,
    "peak_hour_consumption_kwh": _peak_bucket_hourly_value,
    "shiftable_consumption_kwh": _peak_bucket_hourly_value,
    "seasonal_pattern_percent": _seasonal_pattern_percent,
}
