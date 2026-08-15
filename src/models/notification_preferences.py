from datetime import datetime
from .base import Base
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Boolean, DateTime, CheckConstraint, Identity, Index, Integer, Numeric, String, text, Column,Time
from sqlalchemy.dialects.postgresql import ARRAY


class NotificationPreferences(Base):
    __tablename__ = "notification_preferences"
    __table_args__ = (
        CheckConstraint(
            "id = 1",
            name="notification_preferences_id_check",
        ),
        CheckConstraint(
            "quiet_hours_enabled = false OR "
            "(quiet_hours_start IS NOT NULL AND quiet_hours_end IS NOT NULL)",
            name="notification_preferences_check",
        ),
        CheckConstraint(
            "max_retry_attempts >= 0",
            name="notification_preferences_max_retry_attempts_check",
        ),
        CheckConstraint(
            "retry_delay_minutes >= 0",
            name="notification_preferences_retry_delay_minutes_check",
        ),
        CheckConstraint(
            "minimum_severity IN ('INFO', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL')",
            name="notification_preferences_minimum_severity_check",
        ),
        CheckConstraint(
            "push_minimum_severity IN ('INFO', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL')",
            name="notification_preferences_push_minimum_severity_check",
        ),
        CheckConstraint(
            "sms_minimum_severity IN ('INFO', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL')",
            name="notification_preferences_sms_minimum_severity_check",
        ),
        {"schema": "alert"},
    )
    id : Mapped[int] = mapped_column(Integer, Identity(), primary_key=True)
    notifications_enabled: Mapped[bool] = mapped_column(Boolean,nullable=False,server_default=text('true'))
    in_app_enabled : Mapped[bool] = mapped_column(Boolean,nullable=False,server_default=text('true'))
    push_enabled : Mapped[bool] = mapped_column(Boolean,nullable=False,server_default=text('true'))
    sms_enabled : Mapped[bool] = mapped_column(Boolean,nullable=False,server_default=text('false'))
    usage_alerts_enabled : Mapped[bool] = mapped_column(Boolean,nullable=False,server_default=text('true'))
    anomaly_alerts_enabled : Mapped[bool] = mapped_column(Boolean,nullable=False,server_default=text('true'))
    recommendation_alerts_enabled : Mapped[bool] = mapped_column(Boolean,nullable=False,server_default=text('true'))
    system_alerts_enabled : Mapped[bool] = mapped_column(Boolean,nullable=False,server_default=text('true'))
    minimum_severity : Mapped[str] = mapped_column(String(10),nullable=False,server_default=text("'INFO'"))
    push_minimum_severity : Mapped[str] = mapped_column(String(10),nullable=False,server_default=text("'MEDIUM'"))
    sms_minimum_severity : Mapped[str] = mapped_column(String(10),nullable=False,server_default=text("'HIGH'"))
    quiet_hours_enabled : Mapped[bool] = mapped_column(Boolean,nullable=False,server_default=text('false'))
    quiet_hours_start : Mapped[Time | None] = mapped_column(Time(timezone=False),nullable=True)
    quiet_hours_end : Mapped[Time | None] = mapped_column(Time(timezone=False),nullable=True)
    quiet_hours_timezone : Mapped[str] = mapped_column(String(50),nullable=False,server_default=text("'Asia/Tehran'"))
    max_retry_attempts : Mapped[int] = mapped_column(Integer,nullable=False,server_default=text('3'))
    retry_delay_minutes : Mapped[int] = mapped_column(Integer,nullable=False,server_default=text('5'))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))