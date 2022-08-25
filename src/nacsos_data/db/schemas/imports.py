from sqlalchemy import String, ForeignKey, Column, Enum as SAEnum, DateTime, func
from sqlalchemy.orm import relationship
from sqlalchemy_json import mutable_json_type
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB
from uuid import uuid4

from .users import User
from .projects import Project
from ..base_class import Base
from ...models.imports import ImportType


class Import(Base):
    __tablename__ = 'import'

    # Unique identifier for this import
    import_id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4,
                       nullable=False, unique=True, index=True)

    # The user who created this import (may be NULL if done via a script)
    user_id = Column(PG_UUID(as_uuid=True),
                     ForeignKey(User.user_id),  # type: ignore[arg-type, misc] # FIXME
                     nullable=True, index=True, primary_key=False)

    # The project this import is attached to
    project_id = Column(PG_UUID(as_uuid=True),
                        ForeignKey(Project.project_id),  # type: ignore[arg-type, misc] # FIXME
                        nullable=False, index=True, primary_key=False)

    # The task_id assigned by nacsos-pipes service (if this import is handled by a pipeline)
    pipeline_task_id = Column(String, nullable=True, index=True, primary_key=False)

    # Unique descriptive name/title for the import
    name = Column(String, nullable=False)

    # A brief description of that import.
    # Optional, but should be used and can be Markdown formatted
    description = Column(String, nullable=True)

    # Defines what sort of import this is
    type = Column(SAEnum(ImportType), nullable=False)

    # Date and time when this import was created and when the actual import was triggered
    time_created = Column(DateTime(timezone=True), server_default=func.now())
    time_started = Column(DateTime(timezone=True), nullable=True)
    time_finished = Column(DateTime(timezone=True), nullable=True)

    # This stores the configuration of the respective import method
    config = Column(mutable_json_type(dbtype=JSONB, nested=True))  # type: ignore[misc] # FIXME

    # reference to the associated m2m rows
    m2m = relationship('M2MImportItem', cascade='all, delete')  # type: ignore[misc] # FIXME
