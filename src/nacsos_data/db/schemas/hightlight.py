import uuid
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import mapped_column
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from ...db.base_class import Base
from .projects import Project


class Highlighter(Base):
    """

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
