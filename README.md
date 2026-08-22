Flow:






run project:
uv run uvicorn src.main:app                # API
uv run python -m src.kafka.consumer         # Detection Engine (meter readings)
uv run python -m src.kafka.detection_relay  # detection.outbox_events -> Kafka
uv run python -m src.kafka.condition_consumer  # Lifecycle Manager (condition events)
uv run python -m src.kafka.relay            # alert.outbox_events -> Kafka 

input payloud from kafka:

```json
{
  "premises_id": 12345,
  "meter_type": "FAHAM",
  "measured_at": "2025-01-15T10:30:00Z",
  "timezone": "Asia/Tehran",
  "total_kwh_hourly": 5.2,
  ...
}
```

(`timezone` is a required field -- omitting it makes the consumer silently skip the message.)

output payloads on Kafka:

topic `alert.outbox` (published by `python -m src.kafka.relay`), envelope:

```json
{
  "event_id": "<uuid, stable across redeliveries>",
  "event_type": "...",
  "aggregate_type": "ALERT",
  "aggregate_id": 6,
  "payload": { ... },
  "created_at": "..."
}
```

event types and their `payload`:

| event_type | payload |
|---|---|
| `alert.created` / `alert.resolved` / `alert.acknowledged` / `alert.snoozed` | `{alert_id, premises_id, rule_type, severity, state, occurrence_count[, from_state][, snoozed_until]}` |
| `notification.requested` — **input contract of the Notification Service** | `{request_key: "<alert_id>:<occurrence_count>", alert_id, premises_id, rule_type, severity, title, message, current_value, threshold_value, unit, occurrence_count}` |

internal topic `detection.condition.events` (`detection_relay` -> `condition_consumer`, not for external consumers):

```json
{"event_id": "...", "event_type": "ConditionDetected|ConditionCleared", "payload": {...}}
```

{
  "aggregate_type": "ALERT",
  "aggregate_id": 4,
  "event_type": "alert.created",
  "payload": {
    "alert_id": 4,
    "premises_id": 999,
    "rule_type": "DAILY_CONSUMPTION_HIGH",
    "severity": "HIGH",
    "state": "ACTIVE",
    "occurrence_count": 2
  }
