"""
Sends a sequence of fake EnergyReading messages to settings.METER_READINGS_TOPIC
so the running pipeline (consumer.py -> detection_relay.py -> condition_consumer.py
-> relay.py) can be exercised end to end against the real DB/Kafka in .env.

Run this from the project root (alert-service/) so `src` is importable:

    python -m scripts.send_test_readings

Prerequisites (run each in its own terminal first, from the project root):

    python -m src.kafka.consumer
    python -m src.kafka.detection_relay
    python -m src.kafka.condition_consumer
    python -m src.kafka.relay

Each scenario uses its own premises_id in the 90000+ range so results never
collide with real data and are easy to filter/verify in the DB afterwards.
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from aiokafka import AIOKafkaProducer

from src.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Small gap between messages so the pipeline has time to process each one
# and cooldown/occurrence logic (which is time-based) behaves predictably.
STEP_DELAY_SECONDS = 3


def _dec(v) -> str:
    """json.dumps can't serialize Decimal directly; send everything as plain numbers."""
    return float(v) if isinstance(v, Decimal) else v


def reading(
    premises_id: int,
    *,
    total_kwh_hourly: float = 0.5,
    total_kwh_daily: float = 5.0,
    total_kwh_period: float = 100.0,
    pattern_threshold_period: float = 300.0,
    tou_bucket: str | None = "off_peak",
    steps_period: list | None = None,
    current_step: float | None = None,
    next_step: float | None = None,
    cost_period: float = 500000.0,
    measured_at: datetime | None = None,
) -> dict:
    return {
        "premises_id": premises_id,
        "meter_type": "FAHAM",
        "measured_at": (measured_at or datetime.now(timezone.utc)).isoformat(),
        "total_kwh_hourly": total_kwh_hourly,
        "total_kwh_daily": total_kwh_daily,
        "total_kwh_period": total_kwh_period,
        "pattern_threshold_period": pattern_threshold_period,
        "tou_bucket": tou_bucket,
        "steps_period": steps_period or [],
        "current_step": current_step,
        "next_step": next_step,
        "cost_period": cost_period,
    }


# pattern_threshold_period = 300 kWh/month everywhere below, so:
#   daily threshold   = 300 / 30        = 10.0
#   hourly threshold  = 300 / (30*24)   = 0.4166...
#   monthly threshold = 300 (compared directly against total_kwh_period)

SCENARIOS = [
    # --- 1. DAILY_CONSUMPTION_HIGH -----------------------------------
    (
        "daily_high_TRIGGER (90001)",
        reading(90001, total_kwh_daily=12.0, pattern_threshold_period=300.0),
    ),
    (
        "daily_high_STAY_ACTIVE_same_occurrence (90001)",
        reading(90001, total_kwh_daily=13.0, pattern_threshold_period=300.0),
    ),
    (
        "daily_high_CLEAR (90001)",
        reading(90001, total_kwh_daily=8.0, pattern_threshold_period=300.0),
    ),

    # --- 2. MONTHLY_CONSUMPTION_HIGH ----------------------------------
    (
        "monthly_high_TRIGGER (90002)",
        reading(90002, total_kwh_period=350.0, pattern_threshold_period=300.0),
    ),
    (
        "monthly_high_CLEAR (90002)",
        reading(90002, total_kwh_period=200.0, pattern_threshold_period=300.0),
    ),

    # --- 3. HOURLY_CONSUMPTION_HIGH -----------------------------------
    (
        "hourly_high_TRIGGER (90003)",
        reading(90003, total_kwh_hourly=0.9, pattern_threshold_period=300.0),
    ),
    (
        "hourly_high_CLEAR (90003)",
        reading(90003, total_kwh_hourly=0.1, pattern_threshold_period=300.0),
    ),

    # --- 4. PEAK_HOUR_CONSUMPTION_HIGH --------------------------------
    (
        "peak_hour_TRIGGER_in_peak_bucket (90004)",
        reading(90004, total_kwh_hourly=0.9, tou_bucket="peak", pattern_threshold_period=300.0),
    ),
    (
        "peak_hour_NO_TRIGGER_same_value_off_peak (90004b)",
        # Same hourly value as above but NOT in the peak bucket -> extractor
        # returns None for PEAK_HOUR_CONSUMPTION_HIGH, so it must NOT fire.
        # HOURLY_CONSUMPTION_HIGH still fires independently since it doesn't care about tou_bucket.
        reading(90004, total_kwh_hourly=0.9, tou_bucket="off_peak", pattern_threshold_period=300.0),
    ),

    # --- 5. SHIFT_LOAD_TO_OFF_PEAK (derived threshold = pattern/720 = 300/720 ≈ 0.417 kWh) ---
    (
        "shift_load_TRIGGER (90005)",
        reading(90005, total_kwh_hourly=2.5, tou_bucket="peak", pattern_threshold_period=300.0),
    ),
    (
        "shift_load_NO_TRIGGER_below_derived_threshold (90005)",
        reading(90005, total_kwh_hourly=0.3, tou_bucket="peak", pattern_threshold_period=300.0),
    ),
    (
        "shift_load_NO_TRIGGER_not_peak_bucket (90005b)",
        reading(90005, total_kwh_hourly=3.0, tou_bucket="off_peak", pattern_threshold_period=300.0),
    ),

    # --- 6. NEAR_PATTERN_CONSUMPTION (fixed threshold = 90%, GTE) ----
    (
        "near_pattern_NO_TRIGGER_below_90pct (90006)",
        reading(
            90006, total_kwh_period=250.0,
            steps_period=[
                {"step_from": 0.0, "step_to": 300.0},
                {"step_from": 300.0, "step_to": 600.0},
            ],
        ),  # next bound above 250 is 300 -> 250/300 = 83.3% -> below 90
    ),
    (
        "near_pattern_TRIGGER_at_90pct (90006)",
        reading(
            90006, total_kwh_period=280.0,
            steps_period=[
                {"step_from": 0.0, "step_to": 300.0},
                {"step_from": 300.0, "step_to": 600.0},
            ],
        ),  # next bound above 280 is 300 -> 280/300 = 93.3% -> above 90
    ),
    (
        "near_pattern_NO_DATA_missing_steps (90006b)",
        # steps_period absent -> rule must be skipped, not crash.
        reading(90006, total_kwh_period=280.0, pattern_threshold_period=300.0),
    ),

    # --- 7. Disabled rule sanity check (COST_THRESHOLD_EXCEEDED, enabled=false) ---
    (
        "disabled_rule_cost_should_NOT_alert (90007)",
        reading(90007, cost_period=10_000_000.0),
    ),

    # --- 8. Cooldown behavior: same rule twice fast -> 1 alert, occurrence bumps,
    #        only 1 notification (cooldown_minutes=1440 for DAILY_CONSUMPTION_HIGH) ---
    (
        "cooldown_first_trigger (90008)",
        reading(90008, total_kwh_daily=15.0, pattern_threshold_period=300.0),
    ),
    (
        "cooldown_second_trigger_within_window (90008)",
        reading(90008, total_kwh_daily=16.0, pattern_threshold_period=300.0),
    ),
]


async def main() -> None:
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        for label, payload in SCENARIOS:
            logger.info("Sending: %s", label)
            await producer.send_and_wait(
                settings.METER_READINGS_TOPIC,
                key=str(payload["premises_id"]).encode(),
                value=json.dumps(payload).encode(),
            )
            await asyncio.sleep(STEP_DELAY_SECONDS)
        logger.info("All %d scenarios sent to topic %s", len(SCENARIOS), settings.METER_READINGS_TOPIC)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
