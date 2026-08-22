from decimal import Decimal
from src.schemas.input_message import EnergyReading


def _peak_bucket_hourly_value(reading: EnergyReading) -> Decimal | None:
    if reading.tou_bucket != "peak":
        return None
    return reading.total_kwh_hourly


def _seasonal_pattern_percent(reading: EnergyReading) -> Decimal | None:
    """
    Utilization of the seasonal pattern baseline, per spec:
        ratio = total_kwh_period / pattern_threshold_period * 100
    (spec example: 389 kWh vs pattern 300 -> 129.67% = crossed).
    Shared by NEAR_PATTERN_CONSUMPTION (GTE 90) and
    CROSSING_PATTERN_CONSUMPTION (GTE 100).
    """
    if reading.pattern_threshold_period <= 0:
        return None
    return (reading.total_kwh_period / reading.pattern_threshold_period) * Decimal(100)


# Upstream marks "no next tariff step" with this boundary value.
_SENTINEL_STEP_BOUNDARY = Decimal("999999999")


def _tariff_step_usage_percent(reading: EnergyReading) -> Decimal | None:
    """
    Progress toward the next tariff step boundary, per spec:
        usage = total_kwh_period / next_step_boundary * 100
    (spec example: 389 kWh vs boundary 450 -> 86.44%).
    next_step/steps_period carry kWh boundaries; the sentinel boundary
    means "already in the top step" -> no meaningful target.
    """
    total = reading.total_kwh_period
    target = None
    if reading.next_step is not None and reading.next_step < _SENTINEL_STEP_BOUNDARY:
        target = reading.next_step
    if target is None:
        upcoming = [
            s.step_to
            for s in reading.steps_period
            if s.step_to is not None and total < s.step_to < _SENTINEL_STEP_BOUNDARY
        ]
        target = min(upcoming) if upcoming else None
    if target is None or target <= 0:
        return None
    return (total / target) * Decimal(100)


METRIC_EXTRACTORS = {
    "daily_consumption_kwh": lambda r: r.total_kwh_daily,
    "monthly_consumption_kwh": lambda r: r.total_kwh_period,
    "hourly_consumption_kwh": lambda r: r.total_kwh_hourly,
    "peak_hour_consumption_kwh": _peak_bucket_hourly_value,
    "shiftable_consumption_kwh": _peak_bucket_hourly_value,
    "seasonal_pattern_percent": _seasonal_pattern_percent,
    "tariff_step_usage_percent": _tariff_step_usage_percent,
}
