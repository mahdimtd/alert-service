from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from src.database import SessionLocal
from src.detection.engine import process_reading
from src.lifecycle.manager import process_signal, resolve_alert
from src.models.alert_rule import AlertRule
from src.models.outbox_events import OutboxEvent
from src.schemas.alert_signal import AlertSignal
from src.schemas.input_message import EnergyReading, TariffStep

FAKE_PREMISES_ID = 999


def _high_reading() -> EnergyReading:
    return EnergyReading(
        premises_id=FAKE_PREMISES_ID,
        meter_type="FAHAM",
        measured_at=datetime.now(timezone.utc),
        total_kwh_hourly=Decimal("20"),
        total_kwh_daily=Decimal("400"),
        total_kwh_period=Decimal("6000"),
        pattern_threshold_period=Decimal("3000"),
        tou_bucket="peak",
        steps_period=[
            TariffStep(step_from=Decimal("0"), step_to=Decimal("200")),
            TariffStep(step_from=Decimal("200"), step_to=Decimal("500")),
            TariffStep(step_from=Decimal("500"), step_to=Decimal("999999999")),
        ],
        cost_period=Decimal("108298941"),
    )


def main():
    db = SessionLocal()
    try:
        rule = db.query(AlertRule).filter(AlertRule.enabled == True).first()  # noqa: E712
        if rule is None:
            print("No enabled alert rule found in the database. Seed alert_rules first.")
            return

        print(f"Using rule: {rule.type} (severity={rule.severity})\n")

        # 1) Detection Engine: reading -> condition state + ConditionDetected
        outcomes = process_reading(db, _high_reading())
        db.commit()
        print(f"[detection] {outcomes}")

        # 2) Lifecycle (what condition_consumer does with ConditionDetected):
        #    creates/updates the alert and enqueues notification.requested
        #    in the same transaction whenever the policy says notify.
        signal = AlertSignal(
            premises_id=FAKE_PREMISES_ID,
            rule_id=rule.id,
            value=rule.threshold + Decimal("1"),
            threshold=rule.threshold,
        )
        alert, should_notify = process_signal(db, signal)
        db.commit()
        print(f"[lifecycle] alert_id={alert.id} state={alert.state} "
              f"occurrence_count={alert.occurrence_count} should_notify={should_notify}")

        # 3) Verify the notification.requested event landed in the outbox
        stmt = (
            select(OutboxEvent)
            .where(
                OutboxEvent.aggregate_type == "ALERT",
                OutboxEvent.aggregate_id == alert.id,
                OutboxEvent.event_type == "notification.requested",
            )
            .order_by(OutboxEvent.id.desc())
            .limit(1)
        )
        event = db.execute(stmt).scalar_one_or_none()
        if event is None:
            print("[outbox] NO notification.requested row found (unexpected!)")
            return
        print(f"[outbox] notification.requested payload:")
        for k, v in event.payload.items():
            print(f"    {k}: {v}")

        # 4) Clean up: resolve so repeated runs start fresh
        resolved = resolve_alert(db, FAKE_PREMISES_ID, rule.type)
        db.commit()
        if resolved:
            print(f"\n[cleanup] alert {resolved.id} resolved")

        print("\nDone.")
    except Exception as e:
        db.rollback()
        print("Error while running end-to-end test:")
        print(e)
    finally:
        db.close()


if __name__ == "__main__":
    main()
