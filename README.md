uv run uvicorn src.main:app                # API
uv run python -m src.kafka.consumer         # Detection Engine (meter readings)
uv run python -m src.kafka.detection_relay  # detection.outbox_events -> Kafka
uv run python -m src.kafka.condition_consumer  # Lifecycle Manager (condition events)
uv run python -m src.kafka.relay            # alert.outbox_events -> Kafka 