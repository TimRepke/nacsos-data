from __future__ import annotations
from uuid import uuid4
from typing import TYPE_CHECKING
from sqlalchemy import String, ForeignKey, Enum as SAEnum, DateTime, func, Column, Table, Integer, PrimaryKeyConstraint, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import relationship, mapped_column, Mapped, Relationship
from sqlalchemy_json import mutable_json_type
from sqlalchemy.dialects.postgresql import UUID, JSONB

from .users import User
from .projects import Project
from ..base_class import Base
from ...models.imports import M2MImportItemType

if TYPE_CHECKING:
    from .items.base import Item

m2m_import_item_table = Table(
    'm2m_import_item',
    Base.metadata,
    Column('import_id', UUID(as_uuid=True), ForeignKey('import.import_id'), nullable=False, index=True),
    Column('item_id', UUID(as_uuid=True), ForeignKey('item.item_id'), nullable=False, index=True),
    # Import revision this m2m entry was first observed
    Column('first_revision', Integer, nullable=False, server_default='1'),
    # Import revision this m2m entry was last observed
    Column('latest_revision', Integer, nullable=False, server_default='1'),
    # This is a type to specify an entry in the many-to-many relation for items to imports.
    #       - An `explicit` m2m relation is used for cases where the import "explicitly" matched this item.
    #         For example: A tweet or paper matched a keyword specified in the query
    #       - An `implicit` m2m relation is used for cases where the import only "implicitly" includes this item.
    #         For example: A tweet is part of the conversation that contained a specified keyword or an
    #                      article that is referenced by an article that is included "explicitly" in the query.
    Column('type', SAEnum(M2MImportItemType), nullable=False, default=M2MImportItemType.explicit, server_default=M2MImportItemType.explicit),
    PrimaryKeyConstraint('import_id', 'item_id', name='m2m_import_item_pkey'),
    ForeignKeyConstraint(['import_id', 'latest_revision'], ['import_revision.import_id', 'import_revision.import_revision_counter']),
)


class Import(Base):
    __tablename__ = 'import'

    # Unique identifier for this import
    import_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4, nullable=False, unique=True, index=True)

    # The user who created this import (may be NULL if done via a script)
    user_id = mapped_column(UUID(as_uuid=True), ForeignKey(User.user_id), nullable=True, index=True, primary_key=False)

    # The project this import is attached to
    project_id = mapped_column(UUID(as_uuid=True), ForeignKey(Project.project_id, ondelete='CASCADE'), nullable=False, index=True, primary_key=False)

    # Unique descriptive name/title for the import
    name = mapped_column(String, nullable=False)

    # A brief description of that import.
    # Not optional, but can be blank and can be Markdown formatted
    description = mapped_column(String, nullable=False)

    # Defines what sort of import this is
    type = mapped_column(String, nullable=False)

    # Date and time when this import was created and when the actual import was triggered
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now())

    # This stores the configuration of the respective import method
    config = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True))

    # reference to the items
    items: Mapped[list[Item]] = relationship('Item', secondary=m2m_import_item_table, back_populates='imports', cascade='all, delete')

    revisions: Relationship['ImportRevision'] = relationship('ImportRevision', cascade='all, delete')


class ImportRevision(Base):
    __tablename__ = 'import_revision'
    __table_args__ = (UniqueConstraint('import_id', 'import_revision_counter'),)

    # Unique identifier for this import revision
    import_revision_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4, nullable=False, unique=True, index=True)
    # NUmber of this revision within this import
    import_revision_counter = mapped_column(Integer, nullable=False)
    # Date and time when this import was created and when the actual import was triggered
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now())
    # The task_id assigned by nacsos-pipes service (if this import is handled by a pipeline)
    pipeline_task_id = mapped_column(String, nullable=True, index=False, primary_key=False, default=None, server_default=None)

    import_id = mapped_column(UUID(as_uuid=True), ForeignKey(Import.import_id), nullable=False, index=True, primary_key=False)

    # Number of items (raw count) from the source
    num_items_retrieved = mapped_column(Integer, nullable=True, default=None, server_default=None)
    # Size of import (overall) at the end of this revision
    num_items = mapped_column(Integer, nullable=True, default=None, server_default=None)
    # Number of items added in this revision
    num_items_new = mapped_column(Integer, nullable=True, default=None, server_default=None)
    # Number of items detected as duplicate in this revision
    num_items_updated = mapped_column(Integer, nullable=True, default=None, server_default=None)
    # Number of items in last revision but not in this one
    num_items_removed = mapped_column(Integer, nullable=True, default=None, server_default=None)
