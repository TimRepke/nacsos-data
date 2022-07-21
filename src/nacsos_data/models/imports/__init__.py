from datetime import datetime
from typing import Literal
from uuid import UUID

from .. import SBaseModel
from ...db.schemas.imports import ImportType

from .import_config_ris import ImportConfigRIS
from .import_config_twitter import ImportConfigTwitter

ImportTypeLiteral = Literal['ris', 'csv', 'jsonl',
                            'wos', 'scopus', 'ebsco', 'jstor', 'ovid', 'pop',
                            'twitter', 'script']

ImportConfig = ImportConfigRIS | ImportConfigTwitter


class ImportModel(SBaseModel):
    # Unique identifier for this import
    import_id: UUID | str | None = None
    # The user who created this import (may be NULL if done via a script)
    user_id: UUID | str | None = None
    # The project this import is attached to
    project_id: UUID | str

    # Unique descriptive name/title for the import
    name: str

    # A brief description of that import.
    # Optional, but should be used and can be Markdown formatted
    description: str

    # Defines what sort of import this is
    type: ImportTypeLiteral | ImportType

    # Date and time when this import was created and when the actual import was triggered
    time_created: datetime | None = None
    time_started: datetime | None = None
    time_finished: datetime | None = None

    # This stores the configuration of the respective import method
    config: ImportConfig | None = None
