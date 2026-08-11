from decimal import Decimal

from src.database import SessionLocal
from src.detection.engine import process_reading
from src.models.alert_rule import AlertRule
from src.models.alerts import Alert
from src.orchestrator.superviser import create_notifications
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
        print(f"Using rule: {rule.type} (severity={rule.severity})\n")

        high_reading = MeterReading(
            premises_id=FAKE_PREMISES_ID,
            meter_type=meter_type,
            metric=rule.metric,
            value=rule.threshold + Decimal("1"),
        )

        # 1) Detection + Lifecycle
        outcomes = process_reading(db, high_reading)
        db.commit()
        print(f"[detection+lifecycle] {outcomes}")

        # 2) Orchestrator, for every outcome that needs notifying
        for outcome in outcomes:
            if not outcome.get("should_notify"):
                continue
            alert = db.get(Alert, outcome["alert_id"])
            notifications = create_notifications(db, alert)
            db.commit()
            print(f"[orchestrator] alert_id={alert.id} severity={alert.severity} "
                  f"-> channels={[n.channel for n in notifications]}")

        print("\nDone.")

    except Exception as e:
        db.rollback()
        print("Error while running end-to-end test:")
        print(e)
    finally:
        db.close()


if __name__ == "__main__":
    main()