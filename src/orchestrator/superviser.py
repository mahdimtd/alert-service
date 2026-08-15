from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models.alert_audit_logs import AlertAuditLog
from src.models.alert_notifications import AlertNotifications
from src.models.alerts import Alert
from src.models.notification_preferences import NotificationPreferences
from src.outbox import enqueue as enqueue_outbox_event

SEVERITY_ORDER = {"INFO": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}

# Maps each alert_rules.type to the notification_preferences category
# toggle that governs it. Derived directly from the three categories
# defined in the spec (3.1 usage/cost, 3.2 behavioral/anomaly,
# 3.3 recommendation-driven) -- the 27 rule types split cleanly across
# them. No rule type currently maps to "system" (no such category exists
# in alert_rules.type yet); that flag stays unused until one does.
RULE_TYPE_CATEGORY = {
    # 3.1 Usage & cost alerts -> usage_alerts_enabled
    "DAILY_CONSUMPTION_HIGH": "usage",
    "MONTHLY_CONSUMPTION_HIGH": "usage",
    "HOURLY_CONSUMPTION_HIGH": "usage",
    "PEAK_HOUR_CONSUMPTION_HIGH": "usage",
    "COST_THRESHOLD_EXCEEDED": "usage",
    "MONTHLY_BUDGET_RISK": "usage",
    "PROJECTED_BILL_HIGH": "usage",
    "TARIFF_STEP_APPROACHING": "usage",
    # 3.2 Behavioral / anomaly alerts -> anomaly_alerts_enabled
    "NEAR_PATTERN_CONSUMPTION": "anomaly",
    "CROSSING_PATTERN_CONSUMPTION": "anomaly",
    "CONSUMPTION_SPIKE": "anomaly",
    "ABNORMAL_DAILY_PATTERN": "anomaly",
    "ABNORMAL_HOURLY_PATTERN": "anomaly",
    "NIGHT_CONSUMPTION_ABNORMAL": "anomaly",
    "WEEKEND_CONSUMPTION_ABNORMAL": "anomaly",
    "BASELOAD_INCREASED": "anomaly",
    "ZERO_CONSUMPTION_UNEXPECTED": "anomaly",
    "SIMILAR_PREMISE_DEVIATION": "anomaly",
    # 3.3 Smart / recommendation-driven alerts -> recommendation_alerts_enabled
    "SHIFT_LOAD_TO_OFF_PEAK": "recommendation",
    "REDUCE_STANDBY_CONSUMPTION": "recommendation",
    "OPTIMIZE_COOLING_USAGE": "recommendation",
    "OPTIMIZE_HEATING_USAGE": "recommendation",
    "POTENTIAL_DEVICE_LEFT_ON": "recommendation",
    "BUDGET_SAVING_RECOMMENDATION": "recommendation",
    "TARIFF_PLAN_RECOMMENDATION": "recommendation",
    "ENERGY_EFFICIENCY_OPPORTUNITY": "recommendation",
    "PREDICTED_HIGH_BILL_ACTION": "recommendation",
}

_CATEGORY_ENABLED_ATTR = {
    "usage": "usage_alerts_enabled",
    "anomaly": "anomaly_alerts_enabled",
    "recommendation": "recommendation_alerts_enabled",
    "system": "system_alerts_enabled",
}


def category_enabled(prefs: NotificationPreferences, rule_type: str) -> bool:
    category = RULE_TYPE_CATEGORY.get(rule_type)
    if category is None:
        # Unknown/unmapped type: fail open rather than silently swallowing
        # an alert the user might need to see. Should not happen once
        # every type in alert_rules.type is mapped above.
        return True
    return getattr(prefs, _CATEGORY_ENABLED_ATTR[category])

# Severities that are allowed to bypass quiet hours. Not specified in the
# document beyond "low-importance notifications are postponed"; CRITICAL
# getting through is the most conservative reading. Confirm/adjust with
# product before relying on this in production.
QUIET_HOURS_BYPASS_SEVERITIES = {"CRITICAL"}


def get_preferences(db: Session) -> NotificationPreferences:
    prefs = db.get(NotificationPreferences, 1)
    if prefs is None:
        raise RuntimeError("notification_preferences singleton row (id=1) is missing")
    return prefs


def is_quiet_hours(prefs: NotificationPreferences, now_utc: datetime) -> bool:
    if not prefs.quiet_hours_enabled:
        return False

    local_time = now_utc.astimezone(ZoneInfo(prefs.quiet_hours_timezone)).time()
    start, end = prefs.quiet_hours_start, prefs.quiet_hours_end

    if start <= end:
        return start <= local_time < end
    # Range wraps past midnight, e.g. 22:00 -> 07:00
    return local_time >= start or local_time < end


def select_channels(
    prefs: NotificationPreferences, severity: str, rule_type: str, now_utc: datetime
) -> list[str]:
    """Decide which channels an alert of this severity/category should go out on, right now."""
    if not prefs.notifications_enabled:
        return []
    if not category_enabled(prefs, rule_type):
        return []
    if SEVERITY_ORDER[severity] < SEVERITY_ORDER[prefs.minimum_severity]:
        return []

    quiet = is_quiet_hours(prefs, now_utc)
    can_send_now = not quiet or severity in QUIET_HOURS_BYPASS_SEVERITIES

    channels: list[str] = []

    if prefs.in_app_enabled:
        # In-app is passive (shows up next time the user opens the app),
        # so quiet hours don't need to hold it back.
        channels.append("IN_APP")

    if (
        prefs.push_enabled
        and SEVERITY_ORDER[severity] >= SEVERITY_ORDER[prefs.push_minimum_severity]
        and can_send_now
    ):
        channels.append("PUSH")

    if (
        prefs.sms_enabled
        and SEVERITY_ORDER[severity] >= SEVERITY_ORDER[prefs.sms_minimum_severity]
        and can_send_now
    ):
        channels.append("SMS")

    return channels


def _build_idempotency_key(alert: Alert, channel: str) -> str:
    # One notification per (alert, channel, occurrence). If the same
    # occurrence is processed twice, the unique constraint on
    # idempotency_key stops a duplicate row from being created.
    return f"{alert.id}:{channel}:{alert.occurrence_count}"


def create_notifications(db: Session, alert: Alert) -> list[AlertNotifications]:
    """
    Entry point: given an alert that the Lifecycle Manager says needs
    notifying (should_notify=True), work out the channels and create
    PENDING alert_notifications rows for each. Actual delivery is the
    Notification Service's job, not this function's.
    """
    prefs = get_preferences(db)
    now = datetime.now(timezone.utc)
    channels = select_channels(prefs, alert.severity, alert.type, now)

    created: list[AlertNotifications] = []

    for channel in channels:
        idempotency_key = _build_idempotency_key(alert, channel)

        stmt = select(AlertNotifications).where(
            AlertNotifications.idempotency_key == idempotency_key
        )
        if db.execute(stmt).scalar_one_or_none() is not None:
            continue  # already created for this alert/channel/occurrence

        notification = AlertNotifications(
            alert_id=alert.id,
            premises_id=alert.premises_id,
            notification_preferences_id=prefs.id,
            channel=channel,
            idempotency_key=idempotency_key,
        )
        db.add(notification)
        created.append(notification)

    if created:
        db.flush()  # populate notification.id before logging
        for notification in created:
            db.add(AlertAuditLog(
                alert_id=alert.id,
                event="NOTIFICATION_CREATED",
                from_state=None,
                to_state=None,
                actor_type="SYSTEM",
                event_metadata={
                    "channel": notification.channel,
                    "notification_id": notification.id,
                },
            ))
            enqueue_outbox_event(
                db, "NOTIFICATION", notification.id, "notification.requested",
                {
                    "notification_id": notification.id,
                    "alert_id": alert.id,
                    "premises_id": alert.premises_id,
                    "channel": notification.channel,
                    "severity": alert.severity,
                    "idempotency_key": notification.idempotency_key,
                },
            )

    return created