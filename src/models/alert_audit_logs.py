from datetime import datetime
from typing import Any
from sqlalchemy import String,Identity,Index,Integer,CheckConstraint,text,DateTime,ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import mapped_column,Mapped
from .base import Base


class AlertAuditLog(Base):
    __tablename__ = 'alert_audit_logs'
    __table_args__ = (
        CheckConstraint(
            "actor_type IN ('SYSTEM', 'USER', 'WORKER')",
            name="alert_audit_logs_actor_type_check",
        ),
        CheckConstraint(
            "event IN ('ALERT_CREATED', 'ALERT_UPDATED', 'ALERT_ACKNOWLEDGED', "
            "'ALERT_SNOOZED', 'ALERT_REACTIVATED', 'ALERT_RESOLVED', "
            "'NOTIFICATION_CREATED', 'NOTIFICATION_SENT', 'NOTIFICATION_FAILED')",
            name="alert_audit_logs_event_check",
        ),
        Index("idx_alert_audit_logs_alert", "alert_id", "created_at"),
        {"schema": "alert"},
    )
    id : Mapped[int] = mapped_column(Integer,Identity(),nullable=False,primary_key=True)
    alert_id : Mapped[int] = mapped_column(Integer,ForeignKey("alert.alerts.id",ondelete="CASCADE"),nullable=False)
    event : Mapped[str] = mapped_column(String(50),nullable=False)
    from_state : Mapped[str | None] = mapped_column(String(20),nullable=True)
    to_state : Mapped[str | None] = mapped_column(String(20),nullable=True)
    actor_type : Mapped[str] = mapped_column(String(20),nullable=False,server_default=text("'SYSTEM'"))
    #name metadata in db is wronge because its reserved .so we changed in code only.
    event_metadata : Mapped[dict[str, Any]] = mapped_column("metadata",JSONB,nullable=False,server_default=text("'{}'"))
    created_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),nullable=False,server_default=text("now()"))