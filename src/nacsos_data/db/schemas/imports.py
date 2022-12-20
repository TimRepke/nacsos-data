from __future__ import annotations
from sqlalchemy import String, ForeignKey, Enum as SAEnum, DateTime, func
from sqlalchemy.orm import relationship, mapped_column, Relationship
from sqlalchemy_json import mutable_json_type
from sqlalchemy.dialects.postgresql import UUID, JSONB
from uuid import uuid4

from .users import User
from .projects import Project
from .items.base import Item
from ..base_class import Base
from ...models.imports import ImportType, M2MImportItemType


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

    # reference to the associated m2m rows
    m2m: Relationship['M2MImportItem'] = relationship('M2MImportItem', cascade='all, delete')


class M2MImportItem(Base):
    """
    This describes the many-to-many relation between imports and items.
    In other words: It keeps track of which items were imported when and in which context.

      - If an item already existed in the database, this entry should still be added
        to keep track of what this import (e.g. WoS query) covers.
      - An import job may run for a while. In that case, the `time_created` field
        will refer to the time this item was (virtually) imported, not when the import job started.
    """
    __tablename__ = 'm2m_import_item'

    import_id = mapped_column(UUID(as_uuid=True),
                              ForeignKey(Import.import_id),
                              nullable=False, index=True, primary_key=True)
    item_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(Item.item_id),
                            nullable=False, index=True, primary_key=True)

    # Keeps track of when this import took place.
    # Refers to the actual time the item was imported, not when the import operation was started!
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now())

    # This is a type to specify an entry in the many-to-many relation for items to imports.
    #
    #       - An `explicit` m2m relation is used for cases where the import "explicitly" matched this item.
    #         For example: A tweet or paper matched a keyword specified in the query
    #       - An `implicit` m2m relation is used for cases where the import only "implicitly" includes this item.
    #         For example: A tweet is part of the conversation that contained a specified keyword or an
    #                      article that is referenced by an article that is included "explicitly" in the query.
    type = mapped_column(SAEnum(M2MImportItemType), nullable=False,
                         default=M2MImportItemType.explicit, server_default=M2MImportItemType.explicit)
