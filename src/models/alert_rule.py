from datetime import datetime
from decimal import Decimal
from .base import Base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Boolean,DateTime,CheckConstraint,Identity,Index,Integer,Numeric,String,text



class AlertRule(Base):
    __tablename__ = "alert_rules"
    __table_args__ = (
        CheckConstraint(
            "cooldown_minutes >= 0",
            name="alert_rules_cooldown_minutes_check",
        ),
        CheckConstraint(
            "duration_minutes >= 0",
            name="alert_rules_duration_minutes_check",
        ),
        CheckConstraint(
            "threshold >= 0",
            name="alert_rules_threshold_check",
        ),
        CheckConstraint(
            "operator IN ('GT', 'GTE', 'LT', 'LTE', 'EQ')",
            name="alert_rules_operator_check",
        ),
        CheckConstraint(
            "severity IN ('INFO', 'LOW', 'MEDIUM', 'HIGH')",
            name="alert_rules_severity_check",
        ),
        Index("idx_alert_rules_enabled", "enabled"),
        Index(
            "idx_alert_rules_metric",
            "metric",
            postgresql_where=text("enabled = true"),
        ),
        {"schema": "alert"},
    )

    id: Mapped[int] = mapped_column(Integer, Identity(), primary_key=True)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    type: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    metric: Mapped[str] = mapped_column(String(50), nullable=False)
    operator: Mapped[str] = mapped_column(String(5), nullable=False, server_default=text("'GT'"))
    threshold: Mapped[Decimal] = mapped_column(Numeric(14, 4), nullable=False)
    unit: Mapped[str] = mapped_column(String(20), nullable=False)
    severity: Mapped[str] = mapped_column(String(10), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    duration_minutes: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    cooldown_minutes: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("30"))
    suitable_for: Mapped[list[str] | None] = mapped_column(ARRAY(String(50)), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))