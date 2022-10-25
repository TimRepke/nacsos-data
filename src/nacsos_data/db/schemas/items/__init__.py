from sqlalchemy import String, ForeignKey, DateTime, func
from sqlalchemy.orm import mapped_column, as_declarative
from sqlalchemy_json import mutable_json_type
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid

from ...base_class import Base
from ..projects import Project
from ..imports import Import


class Item(Base):
    """
    User represents a person.
    Most entries in the database will be (indirectly) linked to user accounts, so this is
    at the core of access management and ownership.
    """
    __tablename__ = 'item'

    # Unique identifier for this Item.
    item_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                            nullable=False, unique=True, index=True)

    # The text for this item
    #   Tweet: status_text
    #   Paper: abstract
    text = mapped_column(String, nullable=False)

    # any kind of (json-formatted) meta-data
    #   For project marked as "basic" this information may be shown to the user.
    #   Keys with prefix `_` will not be rendered by the frontend though.
    meta = mapped_column(mutable_json_type(dbtype=JSONB, nested=True))

    # FIXME: fundamental question is how to deal with different use cases.
    #        e.g. for papers, text could be the abstract, title,  full-text, paragraphs of full text
    #             and based on context, the same item (?) would point to different texts
    #             alternatively, we view the specific item as the unique reference and Item as the context-sensitive one
    #             which would lead to lots of repeated data though


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
    # Refers to the actual time the item was imported, not when the import ob was started!
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now())


class M2MProjectItem(Base):
    """
    This describes the many-to-many relation between projects and items.
    In other words: It keeps track of which items belong to which project.
    """
    __tablename__ = 'm2m_project_item'

    project_id = mapped_column(UUID(as_uuid=True),
                               ForeignKey(Project.project_id),
                               nullable=False, index=True, primary_key=True)
    item_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(Item.item_id),
                            nullable=False, index=True, primary_key=True)
