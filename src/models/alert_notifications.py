from datetime import datetime
from sqlalchemy import String,Identity,Index,Integer,Boolean,Numeric,Time,CheckConstraint,text,DateTime,ForeignKey,SmallInteger,Text
from sqlalchemy.orm import mapped_column,Mapped
from .base import Base


class AlertNotifications(Base):
    __tablename__ = 'alert_notifications'
    __table_args__ = (
        CheckConstraint(
            "attempt_count >= 0",
            name="alert_notifications_attempt_count_check",
        ),
        CheckConstraint(
            "channel IN ('IN_APP', 'PUSH', 'SMS')",
            name="alert_notifications_channel_check",
        ),
        CheckConstraint(
            "status IN ('PENDING', 'SENT', 'RETRY', 'FAILED', 'CANCELLED')",
            name="alert_notifications_status_check",
        ),
        Index("idx_alert_notifications_alert", "alert_id"),
        Index("idx_alert_notifications_preferences", "notification_preferences_id"),
        Index("idx_alert_notifications_premises", "premises_id", "status"),
        Index(
            "idx_alert_notifications_worker",
            "status",
            "next_attempt_at",
            postgresql_where=text("status IN ('PENDING', 'RETRY')"),
        ),
        {"schema": "alert"},
    )
    id : Mapped[int] = mapped_column(Integer,Identity(),nullable=False,primary_key=True)
    alert_id : Mapped[int] = mapped_column(Integer,ForeignKey("alert.alerts.id",ondelete="CASCADE"),nullable=False)
    premises_id : Mapped[int] = mapped_column(Integer,nullable=False)
    notification_preferences_id : Mapped[int] = mapped_column(SmallInteger,ForeignKey("alert.notification_preferences.id"),nullable=False,server_default=text("1"))
    channel : Mapped[str] = mapped_column(String(20),nullable=False)
    status : Mapped[str] = mapped_column(String(20),nullable=False,server_default=text("'PENDING'"))
    idempotency_key : Mapped[str] = mapped_column(String(255),nullable=False,unique=True)
    attempt_count : Mapped[int] = mapped_column(Integer,nullable=False,server_default=text("0"))
    next_attempt_at : Mapped[datetime | None] = mapped_column(DateTime(timezone=True),nullable=True)
    provider : Mapped[str | None] = mapped_column(String(30),nullable=True)
    provider_message_id : Mapped[str | None] = mapped_column(String(255),nullable=True)
    error_message : Mapped[str | None] = mapped_column(Text,nullable=True)
    sent_at : Mapped[datetime | None] = mapped_column(DateTime(timezone=True),nullable=True)
    created_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),nullable=False,server_default=text("now()"))
    updated_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),nullable=False,server_default=text("now()"))