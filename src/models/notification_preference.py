from datetime import datetime
from .base import Base
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Boolean,DateTime,CheckConstraint,Identity,Index,Integer,Numeric,String,text
from sqlalchemy.dialects.postgresql import ARRAY


class NotificationPreference(Base):
    __tablename__ = 'notification_preference'
    __table_args__ = (
        CheckConstraint(
            ""
        )
    )
