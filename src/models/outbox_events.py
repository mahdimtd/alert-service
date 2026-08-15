from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Identity, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class OutboxEvent(Base):
    """
    Transactional outbox. Domain events (alert.created, alert.resolved,
    notification.requested, ...) get written here in the SAME db
    transaction as the business change that caused them, so we never
    commit a state change without also recording the event -- and never
    "publish" an event whose transaction got rolled back.

    A separate relay process (built alongside the Kafka step) polls rows
    where published_at IS NULL, publishes them, then sets published_at.
    Nothing in this codebase reads from this table yet.
    """

    __tablename__ = "outbox_events"
    __table_args__ = (
        Index(
            "idx_outbox_events_unpublished",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
        {"schema": "alert"},
    )

    id: Mapped[int] = mapped_column(Integer, Identity(), primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(String(20), nullable=False)  # e.g. "ALERT", "NOTIFICATION"
    aggregate_id: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)  # e.g. "alert.created"
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
