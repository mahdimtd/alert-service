from decimal import Decimal
from src.models.alert_rule import AlertRule

THRESHOLD_CALCULATORS = {
    "DAILY_CONSUMPTION_HIGH": lambda period: period / Decimal(30),
    "MONTHLY_CONSUMPTION_HIGH": lambda period: period,
    "HOURLY_CONSUMPTION_HIGH": lambda period: period / Decimal(30 * 24),
    "PEAK_HOUR_CONSUMPTION_HIGH": lambda period: period / Decimal(30 * 24),
    # same derived hourly threshold as PEAK; the tou_bucket=="peak" gate
    # lives in the shiftable_consumption_kwh extractor
    "SHIFT_LOAD_TO_OFF_PEAK": lambda period: period / Decimal(30 * 24),
}

def resolve_threshold(rule: AlertRule, pattern_threshold_period: Decimal) -> Decimal:
    calc = THRESHOLD_CALCULATORS.get(rule.type)
    if calc is not None:
        return calc(pattern_threshold_period)
    return rule.threshold
