from src.database import SessionLocal
from src.models.alert_rule import AlertRule


def main():
    db = SessionLocal()
    try:
        print(" اتصال به دیتابیس برقرار شد\n")

        # خواندن همه قوانین
        rules = db.query(AlertRule).all()

        if not rules:
            print("هیچ قانونی پیدا نشد.")
            return

        print(f"تعداد قوانین: {len(rules)}\n")

        for rule in rules:
            print(
                f"ID: {rule.id} | "
                f"Type: {rule.type} | "
                f"Metric: {rule.metric} | "
                f"Operator: {rule.operator} | "
                f"Threshold: {rule.threshold} | "
                f"Enabled: {rule.enabled}"
            )

        # مثال: فقط قوانین فعال
        print("\n--- فقط قوانین فعال ---")
        active_rules = db.query(AlertRule).filter(AlertRule.enabled == True).all()
        print(f"تعداد قوانین فعال: {len(active_rules)}")

        # مثال: پیدا کردن یک قانون خاص
        print("\n--- جستجوی یک قانون خاص ---")
        rule = (
            db.query(AlertRule)
            .filter(AlertRule.type == "DAILY_CONSUMPTION_HIGH")
            .first()
        )
        if rule:
            print(f"پیدا شد: {rule.name} | threshold = {rule.threshold}")
        else:
            print("قانون پیدا نشد.")

    except Exception as e:
        print(" خطا در اتصال یا کوئری:")
        print(e)
    finally:
        db.close()
        print("\nاتصال بسته شد.")


if __name__ == "__main__":
    main()