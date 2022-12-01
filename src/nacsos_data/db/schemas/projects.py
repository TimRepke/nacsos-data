import uuid
from typing import TYPE_CHECKING
from sqlalchemy import String, ForeignKey, Boolean, Enum as SAEnum, UniqueConstraint
from sqlalchemy.orm import mapped_column, WriteOnlyMapped, relationship, Relationship, Mapped
from sqlalchemy.dialects.postgresql import UUID

from ..base_class import Base
from .users import User
from .items import ItemType

if TYPE_CHECKING:
    from .items.base import Item


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
    project_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                               nullable=False, unique=True, index=True)

    # Unique descriptive name/title for the project
    name = mapped_column(String, unique=True, nullable=False)

    # A brief description of that project.
    # Optional, but should be used and can be Markdown formatted
    description = mapped_column(String, nullable=True)

    # Defines what sort of data this project works with
    # This is used to show item-type specific interface elements and join enriched meta-data
    type = mapped_column(SAEnum(ItemType), nullable=False)

    # ORM reference to all items in the project
    # Note, that they will not be loaded into memory automatically!
    # https://docs.sqlalchemy.org/en/20/orm/large_collections.html
    items: WriteOnlyMapped[list['Item']] = relationship(cascade="all, delete-orphan",
                                                        passive_deletes=True)

    permissions: Relationship['ProjectPermissions'] = relationship('ProjectPermissions', cascade='all, delete')


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
    __table_args__ = (
        UniqueConstraint('project_id', 'user_id'),
    )

    # Unique identifier for this set of permissions
    project_permission_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                                          nullable=False, unique=True, index=True)

    # Refers to the project this permission relates to
    project_id = mapped_column(UUID(as_uuid=True),
                               ForeignKey(Project.project_id),
                               nullable=False, index=True, unique=False)

    # Refers to the User this set of permissions for this project refers to
    user_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(User.user_id),
                            nullable=False, index=True, unique=False)

    # If true, the user has all permissions for this project
    # Note: All other permission settings below will be ignored if set to "true"
    owner = mapped_column(Boolean, nullable=False, default=False)

    # If true, the user has permission to view and export Items associated with this project
    # This does not include annotations, artefacts or other additional data – only raw Items (and respective extension)
    dataset_read = mapped_column(Boolean, nullable=False, default=False)
    # If true, the user has permission to add or remove individual items to this project.
    # Note: This does not refer to the ability to run queries.
    dataset_edit = mapped_column(Boolean, nullable=False, default=False)

    # If true, the user has permission to see the list of queries used in this project
    imports_read = mapped_column(Boolean, nullable=False, default=False)
    # If true, the user has permission to add, edit, and execute queries for this project
    imports_edit = mapped_column(Boolean, nullable=False, default=False)

    # If true, the user has permission to view and export annotations associated with this project
    annotations_read = mapped_column(Boolean, nullable=False, default=False)
    # If true, the user has permission to annotate items in this project (assuming a respective assignment exists)
    annotations_edit = mapped_column(Boolean, nullable=False, default=False)

    # If true, the user has permission to see available pipelines (and their configuration) for this project
    pipelines_read = mapped_column(Boolean, nullable=False, default=False)
    # If true, the user has permission to configure and execute pipelines for this project
    pipelines_edit = mapped_column(Boolean, nullable=False, default=False)

    # If true, the user has permission to see and export pipeline outputs (aka artefacts)
    artefacts_read = mapped_column(Boolean, nullable=False, default=False)
    # If true, the user has permission to edit and delete pipeline outputs (aka artefacts)
    artefacts_edit = mapped_column(Boolean, nullable=False, default=False)

    user: Mapped['User'] = relationship(back_populates='project_permissions')
