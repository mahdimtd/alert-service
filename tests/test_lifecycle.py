from decimal import Decimal

from src.database import SessionLocal
from src.lifecycle.manager import (
    acknowledge_alert,
    process_signal,
    resolve_alert,
    snooze_alert,
)
from src.models.alert_rule import AlertRule
from src.schemas.alert_signal import AlertSignal

FAKE_PREMISES_ID = 999


def main():
    db = SessionLocal()
    try:
        rule = db.query(AlertRule).filter(AlertRule.enabled == True).first()  # noqa: E712
        if rule is None:
            print("No enabled alert rule found in the database. Seed alert_rules first.")
            return

        print(f"Using rule: {rule.type} (threshold={rule.threshold} {rule.unit})\n")

        # 1) First signal above threshold -> should create a new ACTIVE alert
        signal = AlertSignal(
            premises_id=FAKE_PREMISES_ID,
            rule_id=rule.id,
            value=rule.threshold + Decimal("1"),
            threshold=rule.threshold,
        )
        alert, should_notify = process_signal(db, signal)
        db.commit()
        db.refresh(alert)
        print(f"[1] created alert id={alert.id} state={alert.state} "
              f"occurrence_count={alert.occurrence_count} should_notify={should_notify}")

        # 2) Same condition detected again right away -> should update, and
        #    stay quiet if still inside the rule's cooldown window
        alert2, should_notify2 = process_signal(db, signal)
        db.commit()
        db.refresh(alert2)
        print(f"[2] same alert id={alert2.id} state={alert2.state} "
              f"occurrence_count={alert2.occurrence_count} should_notify={should_notify2}")

        # 3) User acknowledges it
        acknowledge_alert(db, alert2.id)
        db.commit()
        db.refresh(alert2)
        print(f"[3] acknowledged -> state={alert2.state}")

        # 4) User snoozes it for a while
        from datetime import datetime, timedelta, timezone
        snooze_alert(db, alert2.id, until=datetime.now(timezone.utc) + timedelta(minutes=30))
        db.commit()
        db.refresh(alert2)
        print(f"[4] snoozed -> state={alert2.state} snoozed_until={alert2.snoozed_until}")

        # 5) Condition detected again while snoozed -> should reactivate
        alert3, should_notify3 = process_signal(db, signal)
        db.commit()
        db.refresh(alert3)
        print(f"[5] reactivated -> state={alert3.state} should_notify={should_notify3}")

        # 6) Condition goes back to normal -> resolve
        resolved = resolve_alert(db, FAKE_PREMISES_ID, rule.type)
        db.commit()
        if resolved:
            db.refresh(resolved)
            print(f"[6] resolved -> state={resolved.state} resolved_at={resolved.resolved_at}")

        print("\nDone.")

    except Exception as e:
        db.rollback()
        print("Error while running lifecycle test:")
        print(e)
    finally:
        db.close()


if __name__ == "__main__":
    main()