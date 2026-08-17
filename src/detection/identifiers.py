"""
Bridges the integer-keyed alert.alert_rules table with the UUID-keyed
detection domain (detection.condition_states / detection.outbox_events).

There is currently no separate "detection rule" catalog with native UUID
identifiers, so we derive a stable UUID for each AlertRule.id
deterministically. This keeps condition_states.rule_id populated with a
real, consistent UUID without requiring a schema change on alert_rules
today. The original integer id is always carried alongside it in the
outbox event payload, since that is what the Lifecycle Manager actually
needs to populate alerts.rule_id.

If a UUID-keyed rule catalog is introduced later, this mapping can be
dropped in favor of a real foreign key.
"""
import uuid

# Fixed, arbitrary namespace for this service. Do not change this value
# once in production -- changing it would make every existing
# condition_states.rule_id stop matching future lookups for the same
# alert_rules.id.
_RULE_ID_NAMESPACE = uuid.UUID("f47b3c68-3f1b-4b9a-9a34-9d6a6a9d6a9d")


def rule_uuid(alert_rule_id: int) -> uuid.UUID:
    """Deterministically derive a UUID for an alert_rules.id integer."""
    return uuid.uuid5(_RULE_ID_NAMESPACE, f"alert_rule:{alert_rule_id}")
