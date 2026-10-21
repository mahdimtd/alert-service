from decimal import Decimal

from src.models.alert_rule import AlertRule
from src.schemas.input_message import EnergyReading


def _daily(reading: EnergyReading) -> Decimal:
    if reading.pattern_threshold_daily is not None:
        return reading.pattern_threshold_daily
    return reading.pattern_threshold_period / Decimal(30)


def _hourly(reading: EnergyReading) -> Decimal:
    if reading.pattern_threshold_hourly is not None:
        return reading.pattern_threshold_hourly
    return reading.pattern_threshold_period / Decimal(30 * 24)


def _period(reading: EnergyReading) -> Decimal:
    return reading.pattern_threshold_period


THRESHOLD_CALCULATORS = {
    "DAILY_CONSUMPTION_HIGH": _daily,
    "MONTHLY_CONSUMPTION_HIGH": _period,
    "HOURLY_CONSUMPTION_HIGH": _hourly,
    "PEAK_HOUR_CONSUMPTION_HIGH": _hourly,
    # same hourly threshold as PEAK; the tou_bucket=="peak" gate
    # lives in the shiftable_consumption_kwh extractor
    "SHIFT_LOAD_TO_OFF_PEAK": _hourly,
}


def resolve_threshold(rule: AlertRule, reading: EnergyReading) -> Decimal:
    calc = THRESHOLD_CALCULATORS.get(rule.type)
    if calc is not None:
        return calc(reading)
    return rule.threshold