import uuid
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import mapped_column
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from ...db.base_class import Base
from .projects import Project


class Highlighter(Base):
    """
    A highlighter can be used in a project to highlight tokens in text to make
    it easier for users to annotate documents when certain keywords are highlighted.

    It is assumed, that a highlighter roughly corresponds to query terms.
    Furthermore, keywords in a highlighter will typically be joined into
    a regular expression group (e.g. "(keyword1|double keyword|wildcar.*)").

    Each matching group will be wrapped in a highlight span in the frontend.
    """
    __tablename__ = 'highlighters'

    # Unique identifier for this Highlighter
    highlighter_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                                   nullable=False, unique=True, index=True)
    # Reference to a project
    project_id = mapped_column(UUID(as_uuid=True), ForeignKey(Project.project_id, ondelete='CASCADE'),
                               nullable=False, index=True)
    # List of keywords (regexes) to match
    keywords = mapped_column(ARRAY(String), nullable=False, index=False)

    # Valid HTML style="..." string (typically sth. like 'background-color: #123456')
    style = mapped_column(String, nullable=True, index=False)
