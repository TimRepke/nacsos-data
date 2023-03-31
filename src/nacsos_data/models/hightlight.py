from uuid import UUID
from pydantic import BaseModel


class HighlighterModel(BaseModel):
    """
    A highlighter can be used in a project to highlight tokens in text to make
    it easier for users to annotate documents when certain keywords are highlighted.

    It is assumed, that a highlighter roughly corresponds to query terms.
    Furthermore, keywords in a highlighter will typically be joined into
    a regular expression group (e.g. "(keyword1|double keyword|wildcar.*)").

    Each matching group will be wrapped in a highlight span in the frontend.
    """
    # Unique identifier for this Highlighter
    highlighter_id: str | UUID
    # Reference to a project
    project_id: str | UUID
    # List of keywords (regexes) to match
    keywords: list[str]
    # Valid HTML style="..." string (typically sth. like 'background-color: #123456')
    style: str | None
