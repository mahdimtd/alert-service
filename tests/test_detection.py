from datetime import datetime, timezone
from decimal import Decimal

from src.database import SessionLocal
from src.detection.engine import process_reading
from src.models.alert_rule import AlertRule
from src.schemas.input_message import EnergyReading

FAKE_PREMISES_ID = 999


def _reading(high: bool) -> EnergyReading:
    """
    One reading feeds every enabled rule, so push every consumption
    metric clearly above (or below) whatever threshold the rules derive
    from pattern_threshold_period (/30 daily, /720 hourly, raw monthly).
    """
    pattern_threshold_period = Decimal("3000")
    scale = Decimal("2") if high else Decimal("0")
    return EnergyReading(
        premises_id=FAKE_PREMISES_ID,
        meter_type="FAHAM",
        measured_at=datetime.now(timezone.utc),
        timezone="Asia/Tehran",
        total_kwh_hourly=Decimal("10") * scale,
        total_kwh_daily=Decimal("200") * scale,
        total_kwh_period=pattern_threshold_period * scale,
        pattern_threshold_period=pattern_threshold_period,
        cost_current=Decimal("15000"),
        cost_hourly=Decimal("7500"),
        cost_daily=Decimal("120000"),
        cost_period=Decimal("3500000"),
    )


def main():
    db = SessionLocal()
    try:
        rule = db.query(AlertRule).filter(AlertRule.enabled == True).first()  # noqa: E712
        if rule is None:
            print("No enabled alert rule found in the database. Seed alert_rules first.")
            return

        print(f"Using rule: {rule.type} (metric={rule.metric}, "
              f"operator={rule.operator}, threshold={rule.threshold} {rule.unit})\n")

        # 1) Reading above threshold -> condition_states row goes
        #    INACTIVE/CLEARED -> ACTIVE; a ConditionDetected row is
        #    written to detection.outbox_events.
        high_reading = _reading(high=True)
        outcomes = process_reading(db, high_reading)
        db.commit()
        print(f"[1] total_kwh_period={high_reading.total_kwh_period} -> {outcomes}")

        # 2) Same high reading again -> condition stays ACTIVE, no new event.
        outcomes2 = process_reading(db, high_reading)
        db.commit()
        print(f"[2] total_kwh_period={high_reading.total_kwh_period} -> {outcomes2}")

        # 3) Reading back to normal -> condition_states row goes
        #    ACTIVE -> CLEARED; a ConditionCleared row is written to
        #    detection.outbox_events.
        normal_reading = _reading(high=False)
        outcomes3 = process_reading(db, normal_reading)
        db.commit()
        print(f"[3] total_kwh_period={normal_reading.total_kwh_period} -> {outcomes3}")

        print(
            "\nDone. So far these rows only exist in "
            "detection.condition_states / detection.outbox_events. Run "
            "`python -m src.kafka.detection_relay` and "
            "`python -m src.kafka.condition_consumer` alongside this to "
            "see them turn into an actual alert.alerts row."
        )

    except Exception as e:
        db.rollback()
        print("Error while running detection test:")
        print(e)
    finally:
        db.close()


if __name__ == "__main__":
    main()
