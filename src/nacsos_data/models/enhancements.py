import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel


class EnhancementModel(BaseModel):
    """Similar to bot_annotation, but without rules"""

    # Unique identifier for this BotAnnotation
    enhancement_id: str | uuid.UUID
    # The Item this assigment refers to
    item_id: str | uuid.UUID

    # Date and time when this enhancement was created (or last changed)
    time_created: datetime | None = None

    # A reference to keep track of what this is (e.g. mordecai)
    key: str

    payload: dict[str, Any]
