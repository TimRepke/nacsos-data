import uuid
from sqlalchemy import String, ForeignKey, UniqueConstraint, Column, ARRAY, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import mapped_column, column_property, Mapped, Relationship, relationship
from sqlalchemy_json import mutable_json_type

from . import ItemType
from .base import Item
from ..projects import Project
from ...base_class import Base


class LexisNexisItem(Item):
    __tablename__ = 'lexis_item'

    item_id = mapped_column(
        UUID(as_uuid=True), ForeignKey(Item.item_id, ondelete='CASCADE'), default=uuid.uuid4, nullable=False, index=True, primary_key=True, unique=True
    )

    # mirror of `Item.project_id` so we can introduce the UniqueConstraint
    # https://docs.sqlalchemy.org/en/20/faq/ormconfiguration.html#i-m-getting-a-warning-or-error-about-implicitly-combining-column-x-under-attribute-y
    project_id: Mapped[uuid.UUID] = column_property(
        Column(UUID(as_uuid=True), ForeignKey(Project.project_id, ondelete='cascade'), index=True, nullable=False), Item.project_id
    )

    # Teaser text
    teaser = mapped_column(String, nullable=True, unique=False, index=False)

    # Authors from the document
    authors = mapped_column(ARRAY(String), nullable=True, index=False)

    sources: Relationship['LexisNexisItemSource'] = relationship('LexisNexisItemSource', cascade='all, delete', back_populates='article')

    __mapper_args__ = {
        'polymorphic_identity': ItemType.lexis,
    }


class LexisNexisItemSource(Base):
    __tablename__ = 'lexis_item_source'
    __table_args__ = (UniqueConstraint('lexis_id', 'item_id'),)

    item_source_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False, unique=True, index=True)

    item_id = mapped_column(UUID(as_uuid=True), ForeignKey(LexisNexisItem.item_id, ondelete='CASCADE'), nullable=False, index=True, unique=False)

    # LexisNexis document ID (e.g. "urn:contentItem:653B-6Y91-JBH6-C1P5-00000-00")
    lexis_id = mapped_column(String, nullable=False, unique=False, index=True)

    name = mapped_column(String, nullable=True, unique=False, index=False)
    title = mapped_column(String, nullable=True, unique=False, index=False)

    section = mapped_column(String, nullable=True, unique=False, index=False)
    jurisdiction = mapped_column(String, nullable=True, unique=False, index=False)
    location = mapped_column(String, nullable=True, unique=False, index=False)
    content_type = mapped_column(String, nullable=True, unique=False, index=False)

    published_at = mapped_column(DateTime(timezone=True), nullable=True, unique=False, index=False)
    updated_at = mapped_column(DateTime(timezone=True), nullable=True, unique=False, index=False)

    # any kind of (json-formatted) meta-data
    #   For project marked as "basic" this information may be shown to the user.
    #   Keys with prefix `_` will not be rendered by the frontend though.
    meta = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True))

    article: Mapped['LexisNexisItem'] = relationship(back_populates='sources')
