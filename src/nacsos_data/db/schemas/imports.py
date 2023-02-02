from __future__ import annotations
from uuid import uuid4
from typing import TYPE_CHECKING
from sqlalchemy import String, ForeignKey, Enum as SAEnum, DateTime, func, Column, Table
from sqlalchemy.orm import relationship, mapped_column, Mapped
from sqlalchemy_json import mutable_json_type
from sqlalchemy.dialects.postgresql import UUID, JSONB

from .users import User
from .projects import Project
from ..base_class import Base
from ...models.imports import ImportType, M2MImportItemType

if TYPE_CHECKING:
    from .items.base import Item

m2m_import_item_table = Table(
    'm2m_import_item',
    Base.metadata,
    Column('import_id', ForeignKey('import.import_id'), nullable=False, primary_key=True, index=True),
    Column('item_id', ForeignKey('item.item_id'), nullable=False, primary_key=True, index=True),
    Column('time_created', DateTime(timezone=True), server_default=func.now()),

    # This is a type to specify an entry in the many-to-many relation for items to imports.
    #       - An `explicit` m2m relation is used for cases where the import "explicitly" matched this item.
    #         For example: A tweet or paper matched a keyword specified in the query
    #       - An `implicit` m2m relation is used for cases where the import only "implicitly" includes this item.
    #         For example: A tweet is part of the conversation that contained a specified keyword or an
    #                      article that is referenced by an article that is included "explicitly" in the query.
    Column('type', SAEnum(M2MImportItemType), nullable=False,
           default=M2MImportItemType.explicit, server_default=M2MImportItemType.explicit)
)


class Import(Base):
    __tablename__ = 'import'

    # Unique identifier for this import
    import_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4,
                              nullable=False, unique=True, index=True)

    # The user who created this import (may be NULL if done via a script)
    user_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(User.user_id),
                            nullable=True, index=True, primary_key=False)

    # The project this import is attached to
    project_id = mapped_column(UUID(as_uuid=True),
                               ForeignKey(Project.project_id),
                               nullable=False, index=True, primary_key=False)

    # The task_id assigned by nacsos-pipes service (if this import is handled by a pipeline)
    pipeline_task_id = mapped_column(String, nullable=True, index=True, primary_key=False)

    # Unique descriptive name/title for the import
    name = mapped_column(String, nullable=False)

    # A brief description of that import.
    # Not optional, but can be blank and can be Markdown formatted
    description = mapped_column(String, nullable=False)

    # Defines what sort of import this is
    type = mapped_column(SAEnum(ImportType), nullable=False)

    # Date and time when this import was created and when the actual import was triggered
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now())
    time_started = mapped_column(DateTime(timezone=True), nullable=True)
    time_finished = mapped_column(DateTime(timezone=True), nullable=True)

    # This stores the configuration of the respective import method
    config = mapped_column(mutable_json_type(dbtype=JSONB, nested=True))

    # reference to the items
    items: Mapped[list[Item]] = relationship(
        secondary=m2m_import_item_table,
        back_populates='imports'
    )
