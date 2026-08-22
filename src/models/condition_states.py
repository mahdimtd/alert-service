from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Index, Integer, Numeric, PrimaryKeyConstraint, String, text,ForeignKey
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ConditionState(Base):
    __tablename__ = "condition_states"
    __table_args__ = (
        PrimaryKeyConstraint(
            "premises_id",
            "rule_id",
            name="condition_states_pk",
        ),
        Index(
            "idx_condition_states_active_lookup",
            "premises_id",
            "rule_id",
            postgresql_where=text("status = 'ACTIVE'"),
        ),
        Index(
            "ux_condition_states_instance_id",
            "condition_instance_id",
            unique=True,
        ),
        CheckConstraint(
            "status IN ('INACTIVE', 'ACTIVE', 'CLEARED')",
            name="condition_states_status_chk",
        ),
        {"schema": "detection"},
    )

    premises_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    rule_id: Mapped[int] = mapped_column(Integer,ForeignKey("alert.alert_rules.id"), nullable=False)
    condition_instance_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        nullable=False,
        server_default=text("gen_random_uuid()"),
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'INACTIVE'")
    )
    first_detected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_observed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    cleared_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    threshold_value: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 6), nullable=True
    )
    rule_version: Mapped[int] = mapped_column(Integer, nullable=False)
    version: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("1")
    )
    # 'metadata' is a reserved word in sqlalchemy so we rename it in code.
    event_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=text("'{}'")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
