import uuid
from sqlalchemy import String, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import mapped_column
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy_json import mutable_json_type

from ...db.base_class import Base
from .items.base import Item


class Enhancement(Base):
    """Similar to bot_annotation, but without rules"""

    __tablename__ = 'enhancement'

    # Unique identifier for this BotAnnotation
    enhancement_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False, unique=True, index=True)
    # The Item this assigment refers to
    item_id = mapped_column(UUID(as_uuid=True), ForeignKey(Item.item_id, ondelete='CASCADE'), nullable=False, index=True)

    # Date and time when this enhancement was created (or last changed)
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now())

    # A reference to keep track of what this is (e.g. mordecai)
    key = mapped_column(String, nullable=False, index=True)

    payload = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True))
