# Alert Service

Consumes meter readings from Kafka, evaluates them against configurable alert rules,
tracks per-(premises, rule) condition state, and emits alert lifecycle events plus
notification requests through a transactional-outbox pattern.


# input payloud:

```json
{
  "premises_id": 1259,
  "total_kwh_period": 0.05,
  "total_kwh_daily": 0.001,
  "total_kwh_hourly": 0.1,
  "total_kwh_current": 0,
  "avg30_period": 0.0012,
  "pattern_status_period": 1,
  "steps_period": [
    {
      "step_from": 0,
      "step_to": 100
    },
    {
      "step_from": 100,
      "step_to": 200
    }
  ],
  "pattern_threshold_period": 200,
  "pattern_threshold_daily": 5,
  "pattern_threshold_hourly": "0.2777777777777778",
  "measured_at": "2026-10-18T12:38:17.790849+00:00",
  "meter_type": "METER",
  "tou_bucket": "mid",
  "cost_period": 1.935376,
  "current_step": 0,
  "next_step": 100
}
```

## Flow

```
                        ┌──────────────────────────────────────────────────────────┐
                        │                      ALERT SERVICE                       │
 energy.alert ────────► │  consumer.py                                             │
 (Kafka, input)         │      │  parse EnergyReading                              │
                        │      ▼                                                   │
                        │  Detection Engine (engine.py)                            │
                        │      │  value vs threshold, per enabled rule             │
                        │      ▼                                                   │
                        │  detection.condition_states   (DB, row-locked)           │
                        │      │  only on INACTIVE→ACTIVE / ACTIVE→CLEARED         │
                        │      ▼                                                   │
                        │  detection.outbox_events      (DB)                       │
                        └───────────────────────────────┬──────────────────────────┘
                                                        │ detection_relay.py
                                                        ▼
                                     detection.condition.events  (Kafka, internal)
                                                        │
                        ┌───────────────────────────────▼──────────────────────────┐
                        │  condition_consumer.py                                   │
                        │      Lifecycle Manager (manager.py)                      │
                        │        ├─ alert.alerts + alert.audit_logs                │
                        │        └─ alert.outbox_events:                           │
                        │             alert.created / .resolved /                  │
                        │             .acknowledged / .snoozed                     │
                        │             notification.requested ◄─ data-only contract │
                        └───────────────────────────────┬──────────────────────────┘
                                                        │ relay.py (+ event_id)
                                                        ▼
 REST API :8000  ◄── FastAPI (main.py)          alert.outbox  (Kafka, output)
 GET /alerts, POST /readings ...                                │
                                                                ▼
                                              Notification Service  [separate]
```

Five independent processes (see *Running* below).

## Rules & Threshold Calculation

Thresholds come either from a derived formula over `pattern_threshold_period`
(the seasonal pattern baseline carried by every reading) or fall back to the
rule row's own `threshold` column. Comparison operator is taken from the rule
row (`GT` / `GTE` / `LT` / `LTE` / `EQ`). `meter_type` plays no role — every
enabled rule is evaluated for every reading.

Enabled rules:

| Rule | Condition | Threshold |
|---|---|---|
| `DAILY_CONSUMPTION_HIGH` | `total_kwh_daily > T` | `T = pattern_threshold_period / 30` |
| `MONTHLY_CONSUMPTION_HIGH` | `total_kwh_period > T` | `T = pattern_threshold_period` |
| `HOURLY_CONSUMPTION_HIGH` | `total_kwh_hourly > T` | `T = pattern_threshold_period / 720` |
| `PEAK_HOUR_CONSUMPTION_HIGH` | `tou_bucket == "peak"` AND `total_kwh_hourly > T` | `T = pattern_threshold_period / 720` |
| `NEAR_PATTERN_CONSUMPTION` | `(total_kwh_period / pattern_threshold_period) × 100 ≥ T` | `T = 90` (rule row) |
| `SHIFT_LOAD_TO_OFF_PEAK` | `tou_bucket == "peak"` AND `total_kwh_hourly > T` | `T = pattern_threshold_period / 720` |

Ready but currently disabled (`UPDATE alert.alert_rules SET enabled=true WHERE type='...'`):

| Rule | Condition | Notes |
|---|---|---|
| `TARIFF_STEP_APPROACHING` | `(total_kwh_period / next_step) × 100 ≥ 90` | fires when consumption is within the top 10% of the next tariff-step boundary; `next_step = 999999999` means "top step reached" → skipped |
| `CROSSING_PATTERN_CONSUMPTION` | `(total_kwh_period / pattern_threshold_period) × 100 ≥ 100` | same ratio as NEAR_PATTERN with a higher bar |

Rules whose metric cannot be extracted from a reading (missing fields,
off-peak for peak-only rules, sentinel step boundaries) are silently skipped
for that reading — never counted as "not met".

## Detection State Machine

One row per `(premises_id, rule_id)` in `detection.condition_states`,
row-locked per reading:

| From | Condition met? | To | Kafka event |
|---|---|---|---|
| INACTIVE / CLEARED | yes | ACTIVE | `ConditionDetected` |
| ACTIVE | yes | ACTIVE *(unchanged)* | — |
| ACTIVE | no | CLEARED | `ConditionCleared` |
| INACTIVE | no | INACTIVE *(unchanged)* | — |

Only genuine transitions produce outbox rows, so replays are harmless no-ops.

## Lifecycle State Machine

Alert states: `ACTIVE → ACKNOWLEDGED / SNOOZED → RESOLVED`.
An open alert is identified by `fingerprint = "<premises_id>:<rule_type>"`;
a partial unique index guarantees at most one open alert per fingerprint.

On `ConditionDetected` (`process_signal`):

| Situation | Effect |
|---|---|
| no open alert | create `ACTIVE` alert + audit `ALERT_CREATED` + outbox `alert.created` **and** `notification.requested` |
| open, inside `cooldown_minutes`, not snoozed | bump `occurrence_count`, update values, audit `ALERT_UPDATED` — **no Kafka event** |
| open, outside cooldown | same updates + audit `ALERT_UPDATED` + fresh `notification.requested` (reminder) |
| open, SNOOZED | reactivate to `ACTIVE`, clear `snoozed_until`, audit `ALERT_REACTIVATED` + `notification.requested` |

On `ConditionCleared`: any open alert → `RESOLVED` + audit `ALERT_RESOLVED` +
outbox `alert.resolved`.

User actions via REST: `POST /alerts/{id}/acknowledge` (ACTIVE/SNOOZED →
ACKNOWLEDGED), `POST /alerts/{id}/snooze` (ACTIVE/ACKNOWLEDGED → SNOOZED);
each writes an audit row and its own outbox event.

## Input

Kafka topic `energy.alert` (one JSON per message):

```json
{
  "premises_id": 1851,
  "meter_type": "\"METER\"",
  "measured_at": "2026-08-22T11:00:00.074117Z",
  "total_kwh_hourly": 4.721032,
  "total_kwh_daily": 0.0,
  "total_kwh_current": 0.0,
  "total_kwh_period": 1961.094733,
  "avg30_period": 1961.094733,
  "pattern_status_period": 2,
  "pattern_threshold_period": 200,
  "tou_bucket": "peak",
  "steps_period": [
    {"step_from": 0.0, "step_to": 200.0, "price_coefficient": 0.5, "unit_price": 6628.0},
    {"step_from": 200.0, "step_to": 300.0, "price_coefficient": 1.5, "unit_price": 19884.0}
  ],
  "current_step": 500.0,
  "next_step": 999999999.0,
  "cost_period": 108298941.31549
}
```

`current_step` / `next_step` are kWh boundaries, not array indexes.
Unparseable messages are logged and skipped; the pipeline never crashes on them.

## Output

Topic `alert.outbox` — envelope published by `relay.py`:

```json
{
  "event_id": "a0732c1d-0ec8-4afc-a8f1-4c510a7a2192",
  "event_type": "notification.requested",
  "aggregate_type": "ALERT",
  "payload": { "...": "see table" },
  "created_at": "2026-08-22T12:57:56.887000+00:00"
}
```

(The Kafka partition key is the alert id, keeping every event of one alert
ordered on a single partition; the id itself travels inside `payload.alert_id`.)

| `event_type` | `payload` |
|---|---|
| `alert.created` / `alert.resolved` / `alert.acknowledged` / `alert.snoozed` | `{alert_id, premises_id, rule_type, severity, state, occurrence_count[, from_state][, snoozed_until]}` |
| `notification.requested` — **contract of the Notification Service** | `{request_key: "<alert_id>:<occurrence_count>", alert_id, premises_id, rule_type, rule_name, severity, current_value, threshold_value, unit, occurrence_count}` |

`request_key` is unique per alert occurrence; `event_id` is stable across
redeliveries. Payloads carry **data only** — user-facing wording is composed
by the consumer.

Internal topic `detection.condition.events` (`detection_relay` →
`condition_consumer`, not for external consumers):

```json
{"event_id": "...", "event_type": "ConditionDetected|ConditionCleared", "payload": {"alert_rule_id": 1, "premises_id": 999, "value": "31.0", "threshold": "30.0", "...": "..."}}
```

## Running


Start each process in its own terminal:

```
uv run uvicorn src.main:app                 # REST API
uv run python -m src.kafka.consumer         # Detection Engine (reads energy.alert)
uv run python -m src.kafka.detection_relay  # detection.outbox_events -> Kafka
uv run python -m src.kafka.condition_consumer  # condition events -> Lifecycle Manager
uv run python -m src.kafka.relay            # alert.outbox_events -> alert.outbox
```

Prerequisites already applied in this environment: `SELECT/INSERT/UPDATE` on
`detection.condition_states` for the DB role, and the `event_id uuid` column +
unique index on `alert.outbox_events`.


Delivery semantics: offsets are committed only after the DB transaction
commits (at-least-once). Replays are safe: detection transitions are no-ops,
lifecycle dedupes via fingerprint/cooldown, and downstream consumers dedupe
on `event_id`.

## Guide for the Notification Service

This service deliberately does **not** decide channels, filter by user
preferences, observe quiet hours, or word messages. The former in-process
*Notification Orchestrator* was removed; its responsibilities now belong to
the Notification Service, which consumes topic `alert.outbox`:

**1. Consume two kinds of facts**

| Event | Meaning | Action |
|---|---|---|
| `notification.requested` | "alert occurrence N needs notifying" | select channels, enqueue deliveries |
| `alert.resolved` / `alert.acknowledged` / `alert.snoozed` | alert closed or silenced | cancel/postpone pending deliveries for that `alert_id` |

**2. Deduplicate (mandatory — delivery is at-least-once)**

* Per envelope: keep a ledger table `processed_events(event_id uuid PRIMARY KEY)`
  and insert inside the **same transaction** as your business action; a unique
  violation ⇒ skip (duplicate redelivery).
* Per meaning: `request_key = "<alert_id>:<occurrence_count>"` identifies one
  notify-request regardless of channel. Derive per-channel keys as
  `{request_key}:{channel}` with a unique constraint on your notifications table.

**3. Decide channels (former orchestrator logic, now yours)**

Reference behavior previously implemented here:

* global kill-switch + category toggles: map `rule_type` → usage / anomaly /
  recommendation preference flags (27 rule types split across the three
  categories; unknown type ⇒ fail open)
* severity gates: global `minimum_severity` (INFO..CRITICAL), plus per-channel
  floors for PUSH and SMS
* quiet hours: timezone-aware window from preferences; CRITICAL bypasses;
  IN_APP always passes (passive channel); PUSH/SMS postponed otherwise

Preference source today: singleton row `alert.notification_preferences(id=1)`
in this service's database. Phase-2 plan: move ownership/API to the
Notification Service.

**4. Compose the wording**

Payloads carry numbers and identifiers only (`rule_type`, `severity`,
`current_value`, `threshold_value`, `unit`, `occurrence_count`). All
user-facing texts — Persian titles/messages per rule type and channel — are
your templates' job.

**5. Own the delivery queue**

Track your own `PENDING → SENT / RETRY / FAILED` lifecycle with retries and
quiet-hours postponement scheduling. This service keeps only the technical
`alerts.title/message` record (NOT NULL columns) — it never delivers anything.

Relationship summary: one-way feed over Kafka (`alert.outbox`), stable IDs
(`event_id`, `request_key`) provided by this side, no shared tables after the
phase-2 preferences move, no REST coupling required.

## REST API — Production Readiness

```
GET  http://localhost:8000/health
POST http://localhost:8000/alerts/acknowledge-all?premises_id={premises_id}
POST http://localhost:8000/alerts/{alert_id}/acknowledge
POST http://localhost:8000/alerts/{alert_id}/snooze
GET  http://localhost:8000/docs
GET  http://localhost:8000/openapi.json
```