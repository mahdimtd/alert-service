from decimal import Decimal

from src.database import SessionLocal
from src.detection.engine import process_reading
from src.models.alert_rule import AlertRule
from src.schemas.input_message import MeterReading

FAKE_PREMISES_ID = 999


def main():
    db = SessionLocal()
    try:
        rule = db.query(AlertRule).filter(AlertRule.enabled == True).first()  # noqa: E712
        if rule is None:
            print("No enabled alert rule found in the database. Seed alert_rules first.")
            return

        meter_type = rule.suitable_for[0]
        print(f"Using rule: {rule.type} (metric={rule.metric}, "
              f"operator={rule.operator}, threshold={rule.threshold} {rule.unit}, "
              f"meter_type={meter_type})\n")

        # 1) Reading above threshold -> condition_states row goes
        #    INACTIVE/CLEARED -> ACTIVE; a ConditionDetected row is
        #    written to detection.outbox_events.
        high_reading = MeterReading(
            premises_id=FAKE_PREMISES_ID,
            meter_type=meter_type,
            metric=rule.metric,
            value=rule.threshold + Decimal("1"),
        )
        outcomes = process_reading(db, high_reading)
        db.commit()
        print(f"[1] reading={high_reading.value} -> {outcomes}")

        # 2) Same high reading again -> condition stays ACTIVE, no new event.
        outcomes2 = process_reading(db, high_reading)
        db.commit()
        print(f"[2] reading={high_reading.value} -> {outcomes2}")

        # 3) Reading back to normal -> condition_states row goes
        #    ACTIVE -> CLEARED; a ConditionCleared row is written to
        #    detection.outbox_events.
        normal_reading = MeterReading(
            premises_id=FAKE_PREMISES_ID,
            meter_type=meter_type,
            metric=rule.metric,
            value=rule.threshold - Decimal("1"),
        )
        outcomes3 = process_reading(db, normal_reading)
        db.commit()
        print(f"[3] reading={normal_reading.value} -> {outcomes3}")

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
