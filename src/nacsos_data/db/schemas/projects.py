from sqlalchemy import String, ForeignKey, Boolean, Column, Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from enum import Enum
import uuid

from ..base_class import Base

from .users import User


class ProjectType(str, Enum):
    basic = 'basic'
    twitter = 'twitter'
    academic = 'academic'
    patents = 'patents'


class Project(Base):
    """
    Project is the basic structural and conceptual place around which all functionality evolves.
    It is essentially a container for a logically connected set of analyses, e.g. all work for a paper.

    Although Items (and subsequently their type-specific extensions) live outside the scope of a project,
    they way they are augmented by annotations and analysis outcomes is always constrained to the scope
    of a Project.
    """
    __tablename__ = 'project'

    # Unique identifier for this project
    project_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                        nullable=False, unique=True, index=True)

    # Unique descriptive name/title for the project
    name = Column(String, unique=True, nullable=False)

    # A brief description of that project.
    # Optional, but should be used and can be Markdown formatted
    description = Column(String, nullable=True)

    # Defines what sort of data this project works with
    # This is used to show item-type specific interface elements and join enriched meta-data
    type = Column(SAEnum(ProjectType), nullable=False)


class ProjectPermissions(Base):
    """
    ProjectPermissions allows to define fine-grained project-level permission management.
    Once such an entry exists, the user is assumed to have very basic access to the respective project.
    A user may become "owner" of a project, which will allow them to do everything and effectively ignoring the
    other more fine-grained permission settings.

    It is assumed, that a user can always see and edit their own contributions (e.g. annotations) but
    by giving them permission to view annotations, they can also see other users' annotations.
    """
    __tablename__ = 'project_permissions'

    # Unique identifier for this set of permissions
    project_permission_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                                   nullable=False, unique=True, index=True)

    # Refers to the project this permission relates to
    project_id = Column(UUID(as_uuid=True),
                        ForeignKey(Project.project_id),  # type: ignore[arg-type] # FIXME
                        nullable=False)

    # Refers to the User this set of permissions for this project refers to
    user_id = Column(UUID(as_uuid=True),
                     ForeignKey(User.user_id),  # type: ignore[arg-type] # FIXME
                     nullable=False, index=True)

    # If true, the user has all permissions for this project
    # Note: All other permission settings below will be ignored if set to "true"
    owner = Column(Boolean, nullable=False, default=False)

    # If true, the user has permission to view and export Items associated with this project
    # This does not include annotations, artefacts or other additional data – only raw Items (and respective extension)
    dataset_read = Column(Boolean, nullable=False, default=False)
    # If true, the user has permission to add or remove individual items to this project.
    # Note: This does not refer to the ability to run queries.
    dataset_edit = Column(Boolean, nullable=False, default=False)

    # If true, the user has permission to see the list of queries used in this project
    imports_read = Column(Boolean, nullable=False, default=False)
    # If true, the user has permission to add, edit, and execute queries for this project
    imports_edit = Column(Boolean, nullable=False, default=False)

    # If true, the user has permission to view and export annotations associated with this project
    annotations_read = Column(Boolean, nullable=False, default=False)
    # If true, the user has permission to annotate items in this project (assuming a respective assignment exists)
    annotations_edit = Column(Boolean, nullable=False, default=False)

    # If true, the user has permission to see available pipelines (and their configuration) for this project
    pipelines_read = Column(Boolean, nullable=False, default=False)
    # If true, the user has permission to configure and execute pipelines for this project
    pipelines_edit = Column(Boolean, nullable=False, default=False)

    # If true, the user has permission to see and export pipeline outputs (aka artefacts)
    artefacts_read = Column(Boolean, nullable=False, default=False)
    # If true, the user has permission to edit and delete pipeline outputs (aka artefacts)
    artefacts_edit = Column(Boolean, nullable=False, default=False)
