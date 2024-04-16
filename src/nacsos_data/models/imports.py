from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID
from pydantic import BaseModel


class ImportModel(BaseModel):
    # Unique identifier for this import
    import_id: UUID | str | None = None
    # The user who created this import (may be NULL if done via a script)
    user_id: UUID | str | None = None
    # The project this import is attached to
    project_id: UUID | str
    # The task_id assigned by nacsos-pipes service (if this import is handled by a pipeline)
    pipeline_task_id: str | None = None

    # Unique descriptive name/title for the import
    name: str

    # A brief description of that import.
    # Can be blank and can be Markdown formatted
    description: str

    # Defines what sort of import this is
    type: str

    # Date and time when this import was created and when the actual import was triggered
    time_created: datetime | None = None
    time_started: datetime | None = None
    time_finished: datetime | None = None

    # This stores the configuration of the respective import method
    config: dict[str, Any] | None = None


class M2MImportItemType(str, Enum):
    """
    This is a type to specify an entry in the many-to-many relation for items to imports.

      - An `explicit` m2m relation is used for cases where the import "explicitly" matched this item.
        For example: A tweet or paper matched a keyword specified in the query
      - An `implicit` m2m relation is used for cases where the import only "implicitly" includes this item.
        For example: A tweet is part of the conversation that contained a specified keyword or an
                     article that is referenced by an article that is included "explicitly" in the query.
    """
    explicit = 'explicit'
    implicit = 'implicit'


__all__ = ['M2MImportItemType', 'ImportModel']
