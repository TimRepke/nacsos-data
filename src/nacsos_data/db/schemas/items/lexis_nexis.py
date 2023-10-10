import uuid
from sqlalchemy import String, ForeignKey, UniqueConstraint, Column, ARRAY, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import mapped_column, column_property, Mapped, Relationship, relationship

from . import ItemType
from .base import Item
from ..projects import Project
from ...base_class import Base


class LexisNexisItem(Item):
    __tablename__ = 'lexis_item'
    __table_args__ = (
        UniqueConstraint('lexis_id', 'project_id'),
    )

    item_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(Item.item_id, ondelete='CASCADE'),
                            default=uuid.uuid4, nullable=False, index=True, primary_key=True, unique=True)

    # mirror of `Item.project_id` so we can introduce the UniqueConstraint
    # https://docs.sqlalchemy.org/en/20/faq/ormconfiguration.html#i-m-getting-a-warning-or-error-about-implicitly-combining-column-x-under-attribute-y
    project_id: Mapped[uuid.UUID] = column_property(Column(UUID(as_uuid=True),  # type: ignore[assignment]
                                                           ForeignKey(Project.project_id, ondelete='cascade'),
                                                           index=True, nullable=False), Item.project_id)

    # LexisNexis document ID (e.g. "urn:contentItem:653B-6Y91-JBH6-C1P5-00000-00")
    lexis_id = mapped_column(String, nullable=False, unique=False, index=True)

    # Teaser text
    teaser = mapped_column(String, nullable=True, unique=False, index=False)

    # Authors from the document
    authors = mapped_column(ARRAY(String), nullable=True, index=True)

    sources: Relationship['LexisNexisItemSource'] = relationship('LexisNexisItemSource',
                                                                 cascade='all, delete')

    __mapper_args__ = {
        'polymorphic_identity': ItemType.lexis,
    }


class LexisNexisItemSource(Base):
    __tablename__ = 'lexis_item_source'

    item_source_id = mapped_column(UUID(as_uuid=True),
                                   primary_key=True, default=uuid.uuid4,
                                   nullable=False, unique=True, index=True)

    item_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(LexisNexisItem.item_id, ondelete='CASCADE'),
                            nullable=False, index=True, unique=False)
    name = mapped_column(String, nullable=True, unique=False, index=False)
    title = mapped_column(String, nullable=True, unique=False, index=False)

    section = mapped_column(String, nullable=True, unique=False, index=False)
    jurisdiction = mapped_column(String, nullable=True, unique=False, index=False)
    location = mapped_column(String, nullable=True, unique=False, index=False)
    content_type = mapped_column(String, nullable=True, unique=False, index=False)

    published_at = mapped_column(DateTime(timezone=True), nullable=True, unique=False, index=False)
    updated_at = mapped_column(DateTime(timezone=True), nullable=True, unique=False, index=False)
