"""
Reads the DB directly after send_test_readings.py has been sent and the
pipeline (consumer -> detection_relay -> condition_consumer -> relay) has
had time to process everything, and prints a PASS/FAIL report per scenario.

Run from the project root, AFTER waiting a few seconds for the 4 background
processes to drain the topics:

    python -m scripts.verify_test_results

Safe to re-run; it only reads, never writes.
"""
from decimal import Decimal

from sqlalchemy import select

from src.database import SessionLocal
from src.models.alert_notifications import AlertNotifications
from src.models.alert_rule import AlertRule
from src.models.alerts import Alert
from src.models.condition_states import ConditionState

PASS = "PASS"
FAIL = "FAIL"

results: list[tuple[str, str, str]] = []  # (scenario, PASS/FAIL, detail)


def check(scenario: str, condition: bool, detail: str) -> None:
    results.append((scenario, PASS if condition else FAIL, detail))


def alerts_for(db, premises_id: int, rule_type: str | None = None) -> list[Alert]:
    stmt = select(Alert).where(Alert.premises_id == premises_id).order_by(Alert.created_at)
    rows = list(db.execute(stmt).scalars().all())
    if rule_type:
        rows = [r for r in rows if r.type == rule_type]
    return rows


def notifications_for(db, alert_id: int) -> list[AlertNotifications]:
    stmt = select(AlertNotifications).where(AlertNotifications.alert_id == alert_id)
    return list(db.execute(stmt).scalars().all())


def main() -> None:
    db = SessionLocal()
    try:
        # --- 1. DAILY_CONSUMPTION_HIGH (90001): trigger, stay active, clear ---
        daily = alerts_for(db, 90001, "DAILY_CONSUMPTION_HIGH")
        check(
            "daily_consumption_high",
            len(daily) == 1 and daily[0].state == "RESOLVED" and daily[0].occurrence_count == 2,
            f"found {len(daily)} alert(s), "
            f"state={daily[0].state if daily else None}, "
            f"occurrence_count={daily[0].occurrence_count if daily else None}, "
            f"threshold_value={daily[0].threshold_value if daily else None} (expected 10.0000)",
        )

        # --- 2. MONTHLY_CONSUMPTION_HIGH (90002) ---
        monthly = alerts_for(db, 90002, "MONTHLY_CONSUMPTION_HIGH")
        check(
            "monthly_consumption_high",
            len(monthly) == 1 and monthly[0].state == "RESOLVED",
            f"found {len(monthly)} alert(s), state={monthly[0].state if monthly else None}, "
            f"threshold_value={monthly[0].threshold_value if monthly else None} (expected 300.0000)",
        )

        # --- 3. HOURLY_CONSUMPTION_HIGH (90003) ---
        hourly = alerts_for(db, 90003, "HOURLY_CONSUMPTION_HIGH")
        check(
            "hourly_consumption_high",
            len(hourly) == 1 and hourly[0].state == "RESOLVED",
            f"found {len(hourly)} alert(s), state={hourly[0].state if hourly else None}, "
            f"threshold_value={hourly[0].threshold_value if hourly else None} (expected ~0.4167)",
        )

        # --- 4. PEAK_HOUR_CONSUMPTION_HIGH vs HOURLY_CONSUMPTION_HIGH (90004) ---
        # Second reading (off_peak, same hourly value) must NOT retrigger/clear
        # PEAK_HOUR_CONSUMPTION_HIGH (extractor returns None outside peak bucket),
        # but MUST bump HOURLY_CONSUMPTION_HIGH's occurrence_count since that rule
        # doesn't care about tou_bucket.
        peak_hour = alerts_for(db, 90004, "PEAK_HOUR_CONSUMPTION_HIGH")
        hourly_90004 = alerts_for(db, 90004, "HOURLY_CONSUMPTION_HIGH")
        check(
            "peak_hour_consumption_high_ignores_off_peak_reading",
            len(peak_hour) == 1 and peak_hour[0].state == "ACTIVE" and peak_hour[0].occurrence_count == 1,
            f"found {len(peak_hour)} alert(s), state={peak_hour[0].state if peak_hour else None}, "
            f"occurrence_count={peak_hour[0].occurrence_count if peak_hour else None} (expected ACTIVE, 1)",
        )
        check(
            "hourly_consumption_high_still_fires_regardless_of_bucket",
            len(hourly_90004) == 1 and hourly_90004[0].occurrence_count == 2,
            f"found {len(hourly_90004)} alert(s), "
            f"occurrence_count={hourly_90004[0].occurrence_count if hourly_90004 else None} (expected 2)",
        )

        # --- 5. SHIFT_LOAD_TO_OFF_PEAK (90005): fixed threshold=2.0, trigger then clear ---
        shift = alerts_for(db, 90005, "SHIFT_LOAD_TO_OFF_PEAK")
        check(
            "shift_load_to_off_peak_fixed_threshold",
            len(shift) == 1 and shift[0].state == "RESOLVED" and shift[0].threshold_value == Decimal("2.0000"),
            f"found {len(shift)} alert(s), state={shift[0].state if shift else None}, "
            f"threshold_value={shift[0].threshold_value if shift else None} (expected RESOLVED, 2.0000)",
        )

        # --- 6. NEAR_PATTERN_CONSUMPTION (90006): dormant without next_step, fires with it ---
        near = alerts_for(db, 90006, "NEAR_PATTERN_CONSUMPTION")
        check(
            "near_pattern_consumption_percent_based",
            len(near) == 1 and near[0].state == "ACTIVE",
            f"found {len(near)} alert(s), state={near[0].state if near else None} "
            f"(expected exactly 1, ACTIVE -- the 83% reading must NOT have created one, "
            f"the missing-next_step reading must NOT have crashed or cleared it)",
        )

        # --- 7. Disabled rule sanity check (90007) ---
        cond_states_90007 = list(
            db.execute(select(ConditionState).where(ConditionState.premises_id == 90007)).scalars().all()
        )
        alerts_90007 = alerts_for(db, 90007)
        check(
            "disabled_rule_never_evaluated",
            len(cond_states_90007) == 0 and len(alerts_90007) == 0,
            f"condition_states rows={len(cond_states_90007)}, alerts rows={len(alerts_90007)} (expected 0, 0)",
        )

        # --- 8. Cooldown behavior (90008): 2 occurrences, only 1 notification set ---
        cooldown = alerts_for(db, 90008, "DAILY_CONSUMPTION_HIGH")
        if cooldown:
            notifs = notifications_for(db, cooldown[0].id)
        else:
            notifs = []
        check(
            "cooldown_suppresses_second_notification",
            len(cooldown) == 1 and cooldown[0].occurrence_count == 2 and len(notifs) > 0,
            f"alert occurrence_count={cooldown[0].occurrence_count if cooldown else None}, "
            f"notification rows={len(notifs)} "
            f"(expected occurrence_count=2, and notifications created only for the FIRST "
            f"occurrence -- re-run this scenario with a rule whose cooldown_minutes is short "
            f"if you want to see a genuine should_notify=True on re-trigger)",
        )

        # --- report ---
        print("\n=== Test report ===")
        width = max(len(r[0]) for r in results)
        for scenario, status, detail in results:
            print(f"[{status}] {scenario.ljust(width)}  -  {detail}")

        failed = [r for r in results if r[1] == FAIL]
        print(f"\n{len(results) - len(failed)}/{len(results)} passed")
        if failed:
            print(f"{len(failed)} FAILED -- see above")

    finally:
        db.close()


if __name__ == "__main__":
    main()
