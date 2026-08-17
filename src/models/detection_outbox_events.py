from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Identity, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class DetectionOutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        Index(
            "idx_outbox_events_pending",
            text("next_retry_at NULLS FIRST"),
            "id",
            postgresql_where=text("status IN ('PENDING', 'FAILED')"),
        ),
        Index(
            "idx_outbox_events_unpublished",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index(
            "outbox_events_event_id_uk",
            "event_id",
            unique=True,
        ),
        CheckConstraint(
            "(status = 'PUBLISHED' AND published_at IS NOT NULL) OR "
            "(status IN ('PENDING', 'FAILED') AND published_at IS NULL)",
            name="outbox_events_publish_chk",
        ),
        CheckConstraint(
            "status IN ('PENDING', 'PUBLISHED', 'FAILED')",
            name="outbox_events_status_chk",
        ),
        {"schema": "detection"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger, Identity(always=True), primary_key=True
    )
    event_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        nullable=False,
        server_default=text("gen_random_uuid()"),
    )
    aggregate_type: Mapped[str] = mapped_column(String(50), nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    premises_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    rule_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    condition_key: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    topic: Mapped[str] = mapped_column(String(100), nullable=False)
    partition_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'PENDING'")
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
