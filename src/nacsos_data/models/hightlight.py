from uuid import UUID
from pydantic import BaseModel


class Highlighter(BaseModel):
    # Unique identifier for this Highlighter
    highlighter_id: str | UUID
    # Reference to a project
    project_id: str | UUID
    # List of keywords (regexes) to match
    keywords: list[str]
