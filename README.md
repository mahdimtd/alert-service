uv run uvicorn src.main:app --reload      # فقط API (ack/snooze/list/get)
uv run python -m src.kafka.consumer       # فقط گوش دادن به reading ها
uv run python -m src.kafka.relay          # فقط publish کردن outbox