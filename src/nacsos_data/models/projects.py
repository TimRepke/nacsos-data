import uuid
from typing import Literal
from uuid import UUID
import datetime
from pydantic import BaseModel

from nacsos_data.db.schemas.items import ItemType, ItemTypeLiteral


class ProjectModel(BaseModel):
    """
    Project is the basic structural and conceptual place around which all functionality evolves.
    It is essentially a container for a logically connected set of analyses, e.g. all work for a paper.

    Although Items (and subsequently their type-specific extensions) live outside the scope of a project,
    they way they are augmented by annotations and analysis outcomes is always constrained to the scope
    of a Project.
    """

    # Unique identifier for this project
    project_id: str | UUID | None = None

    # Unique descriptive name/title for the project
    name: str

    # A brief description of that project.
    # Optional, but should be used and can be Markdown formatted
    description: str | None = None

    # Date and time when this project was created
    time_created: datetime.datetime | None = None

    # Defines what sort of data this project works with
    # This is used to show item-type specific interface elements and join enriched meta-data
    type: ItemTypeLiteral | ItemType

    # This is a mutex to prevent parallel imports for a project
    # If this is TRUE, DO NOT RUN ANOTHER IMPORT!
    import_mutex: bool | None = None

    # === Project settings ===
    # True = motivational quotes are shown to the user
    setting_motivational_quotes: bool = True


ProjectPermission = Literal[
    'owner',
    'dataset_read', 'dataset_edit',
    'imports_read', 'imports_edit',
    'annotations_read', 'annotations_edit', 'annotations_prio',
    'pipelines_read', 'pipelines_edit',
    'artefacts_read', 'artefacts_edit',
    'search_dimensions', 'search_oa'
]


class ProjectPermissionsModel(BaseModel):
    """
    ProjectPermissions allows to define fine-grained project-level permission management.
    Once such an entry exists, the user is assumed to have very basic access to the respective project.
    A user may become "owner" of a project, which will allow them to do everything and effectively ignoring the
    other more fine-grained permission settings.

    It is assumed, that a user can always see and edit their own contributions (e.g. annotations) but
    by giving them permission to view annotations, they can also see other users' annotations.
    """
    # Unique identifier for this set of permissions
    project_permission_id: str | UUID | None = None

    # Refers to the project this permission relates to
    project_id: str | UUID

    # Refers to the User this set of permissions for this project refers to
    user_id: str | UUID

    # If true, the user has all permissions for this project
    # Note: All other permission settings below will be ignored if set to "true"
    owner: bool = False

    # If true, the user has permission to view and export Items associated with this project
    # This does not include annotations, artefacts or other additional data – only raw Items (and respective extension)
    dataset_read: bool = False
    # If true, the user has permission to add or remove individual items to this project.
    # Note: This does not refer to the ability to run queries.
    dataset_edit: bool = False

    # If true, the user has permission to see the list of queries used in this project
    imports_read: bool = False
    # If true, the user has permission to add, edit, and execute queries for this project
    imports_edit: bool = False

    # If true, the user has permission to view and export annotations associated with this project
    annotations_read: bool = False
    # If true, the user has permission to annotate items in this project (assuming a respective assignment exists)
    annotations_edit: bool = False
    # If true, the user has permission to run prioritisation machine learning
    annotations_prio: bool = False

    # If true, the user has permission to see available pipelines (and their configuration) for this project
    pipelines_read: bool = False
    # If true, the user has permission to configure and execute pipelines for this project
    pipelines_edit: bool = False

    # If true, the user has permission to see and export pipeline outputs (aka artefacts)
    artefacts_read: bool = False
    # If true, the user has permission to edit and delete pipeline outputs (aka artefacts)
    artefacts_edit: bool = False

    # If true, the user has permission to use our dimensions access for search (excl. full query/import)
    search_dimensions: bool = False
    # If true, the user has permission to use our OpenAlex database for search (excl. full query/import)
    search_oa: bool = False

    # Max. number of items per import from OpenAlex (0 = direct OpenAlex import not allowed)
    # This should only be editable by superusers!
    import_limit_oa: int = 0

    @classmethod
    def get_virtual_admin(cls, project_id: str | uuid.UUID, user_id: str | uuid.UUID) -> 'ProjectPermissionsModel':
        return cls(project_permission_id=None, project_id=project_id,
                   user_id=user_id, owner=True,
                   dataset_read=True, dataset_edit=True,
                   imports_read=True, imports_edit=True,
                   annotations_read=True, annotations_edit=True, annotations_prio=True,
                   pipelines_read=True, pipelines_edit=True,
                   artefacts_read=True, artefacts_edit=True,
                   search_oa=True, search_dimensions=True,
                   import_limit_oa=10000)
