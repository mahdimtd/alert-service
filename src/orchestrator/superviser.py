from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models.alert_audit_logs import AlertAuditLog
from src.models.alert_notifications import AlertNotifications
from src.models.alerts import Alert
from src.models.notification_preferences import NotificationPreferences

SEVERITY_ORDER = {"INFO": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}

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
    prefs: NotificationPreferences, severity: str, now_utc: datetime
) -> list[str]:
    """
    Decide which channels an alert of this severity should go out on,
    right now, given the current preferences.

    NOTE: this does not yet filter by alert category (usage_alerts_enabled,
    anomaly_alerts_enabled, recommendation_alerts_enabled,
    system_alerts_enabled). Doing that needs a mapping from alert_rules.type
    to one of those four categories, which isn't defined anywhere in the
    schema yet -- worth deciding explicitly rather than guessing it from
    the type name.
    """
    if not prefs.notifications_enabled:
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
    channels = select_channels(prefs, alert.severity, now)

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

    return created