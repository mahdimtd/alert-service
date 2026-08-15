from datetime import datetime
from decimal import Decimal
from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import Integer,text,Boolean,String,Identity,Index,Numeric,Time,CheckConstraint,DateTime,ForeignKey,Text
from .base import Base


class Alert(Base):
    __tablename__ = 'alerts'
    __table_args__ = (
        CheckConstraint(
            "occurrence_count > 0",
            name="alerts_occurrence_count_check",
        ),
        CheckConstraint(
            "severity IN ('INFO', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL')",
            name="alerts_severity_check",
        ),
        CheckConstraint(
            "state IN ('ACTIVE', 'ACKNOWLEDGED', 'SNOOZED', 'RESOLVED')",
            name="alerts_state_check",
        ),
        Index("idx_alerts_premises_created", "premises_id", "created_at"),
        Index("idx_alerts_premises_state", "premises_id", "state"),
        Index("idx_alerts_rule", "rule_id"),
        Index(
            "uq_alerts_open_fingerprint",
            "premises_id",
            "fingerprint",
            unique=True,
            postgresql_where=text(
                "state IN ('ACTIVE', 'ACKNOWLEDGED', 'SNOOZED')"
            ),
        ),
        {"schema": "alert"},
    )
    id : Mapped[int] = mapped_column(Integer,Identity(),nullable=False,primary_key=True)
    premises_id : Mapped[int] = mapped_column(Integer,nullable=False)
    rule_id : Mapped[int] = mapped_column(Integer,ForeignKey("alert.alert_rules.id"),nullable=False)
    type : Mapped[str] = mapped_column(String(50),nullable=False)
    severity : Mapped[str] = mapped_column(String(10),nullable=False)
    state : Mapped[str] = mapped_column(String(20),nullable=False,server_default=text("'ACTIVE'"))
    title : Mapped[str] = mapped_column(String(200),nullable=False)
    message : Mapped[str] = mapped_column(Text,nullable=False)
    current_value : Mapped[Decimal | None] = mapped_column(Numeric(14,4),nullable=True)
    threshold_value : Mapped[Decimal | None] = mapped_column(Numeric(14,4),nullable=True)
    unit : Mapped[str | None] = mapped_column(String(20),nullable=True)
    fingerprint : Mapped[str] = mapped_column(String(255),nullable=False)
    occurrence_count : Mapped[int] = mapped_column(Integer,nullable=False,server_default=text("1"))
    first_detected_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),nullable=False,server_default=text("now()"))
    last_detected_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),nullable=False,server_default=text("now()"))
    acknowledged_at : Mapped[datetime | None] = mapped_column(DateTime(timezone=True),nullable=True)
    snoozed_until : Mapped[datetime | None] = mapped_column(DateTime(timezone=True),nullable=True)
    resolved_at : Mapped[datetime | None] = mapped_column(DateTime(timezone=True),nullable=True)
    created_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),nullable=False,server_default=text("now()"))
    updated_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),nullable=False,server_default=text("now()"))