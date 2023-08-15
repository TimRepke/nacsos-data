from datetime import datetime
from typing import Literal
from enum import Enum
from uuid import UUID
from pydantic import BaseModel

from nacsos_data.models.pipeline.lib import ImportConfig


class ImportType(str, Enum):
    # File import
    ris = 'ris'  # single or bulk import of publications via RIS file(s)
    csv = 'csv'  # single or bulk import of publications via CSV file(s)
    jsonl = 'jsonl'  # single or bulk import of publications via JSON.l file(s)

    # Scholarly databases
    wos = 'wos'  # Import via Web of Science query
    scopus = 'scopus'  # Import via Scopus query
    ebsco = 'ebsco'  # Import via EBSCO query
    jstor = 'jstor'  # Import via JSTOR query
    ovid = 'ovid'  # Import via OVID query
    pop = 'pop'  # Import via Publish or Perish query

    # Others
    twitter = 'twitter'  # Import via Twitter
    script = 'script'  # Import was done with a script


ImportTypeLiteral = Literal[
    'ris', 'csv', 'jsonl',
    'wos', 'scopus', 'ebsco', 'jstor', 'ovid', 'pop',
    'twitter', 'script']


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
    type: ImportTypeLiteral | ImportType

    # Date and time when this import was created and when the actual import was triggered
    time_created: datetime | None = None
    time_started: datetime | None = None
    time_finished: datetime | None = None

    # This stores the configuration of the respective import method
    config: ImportConfig | None = None


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


__all__ = ['M2MImportItemType', 'ImportType', 'ImportModel', 'ImportTypeLiteral',
           'ImportConfig']
