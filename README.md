Flow:

alert.input  ──► Detection Engine ──► detection.outbox_events 
                            consumer.py          │
                                                 ▼
                              detection_relay.py ──►  detection.condition.events 
                                                 │
                                                 ▼ (internal handoff)
                              condition_consumer.py
                               ├─► Lifecycle Manager ──►  alert.alerts،
                               │    alert_audit_logs و alert.outbox_events
                               │
                               └─► Orchestrator ──►  alert.alert_notifications
                                    +  notification.requested  alert.outbox_events

alert.outbox_events ──► relay.py ──►  alert.outbox  




run project:
uv run uvicorn src.main:app                # API
uv run python -m src.kafka.consumer         # Detection Engine (meter readings)
uv run python -m src.kafka.detection_relay  # detection.outbox_events -> Kafka
uv run python -m src.kafka.condition_consumer  # Lifecycle Manager (condition events)
uv run python -m src.kafka.relay            # alert.outbox_events -> Kafka 

input payloud from kafka:

{
  "premises_id": 12345,
  "meter_type": "FAHAM",
  "measured_at": "2025-01-15T10:30:00Z",
  "total_kwh_hourly": 5.2,
  "total_kwh_daily": 120.5,
  "total_kwh_period": 3500.75,
  "pattern_threshold_period": 3000.00,
  "tou_bucket": "peak",
  "steps_period": [100, 200, 300],
  "current_step": 2,
  "next_step": 3,
  "cost_current": 15000.00,
  "cost_hourly": 7500.00,
  "cost_daily": 120000.00,
  "cost_period": 3500000.00
}

input payloud from API :



output payloud:
