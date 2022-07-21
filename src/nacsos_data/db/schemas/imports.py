from sqlalchemy import String, ForeignKey, Boolean, Column, Enum as SAEnum, DateTime, func, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy_json import mutable_json_type
from sqlalchemy.dialects.postgresql import UUID, JSONB
from enum import Enum
import uuid

from .users import User
from .projects import Project
from ..base_class import Base


class ImportType(Enum):
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


class Import(Base):
    __tablename__ = 'import'

    # Unique identifier for this import
    import_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                       nullable=False, unique=True, index=True)

    # The user who created this import (may be NULL if done via a script)
    user_id = Column(UUID(as_uuid=True), ForeignKey(User.user_id),
                     nullable=True, index=True, primary_key=False)

    # The project this import is attached to
    project_id = Column(UUID(as_uuid=True), ForeignKey(Project.project_id),
                        nullable=False, index=True, primary_key=False)

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
    config = Column(mutable_json_type(dbtype=JSONB, nested=True))

    # reference to the associated m2m rows
    m2m = relationship('M2MImportItem', cascade='all, delete')
