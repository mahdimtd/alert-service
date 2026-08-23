"""Import every model module so all tables/FK targets register on the
shared Base metadata, regardless of which subset a given process uses."""
from . import (  # noqa: F401
    alert_audit_logs,
    alert_notifications,
    alert_rule,
    alerts,
    condition_states,
    detection_outbox_events,
    notification_preferences,
    outbox_events,
)
