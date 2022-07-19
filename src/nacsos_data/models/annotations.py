from typing import Literal, ForwardRef, Optional
from datetime import datetime
from uuid import UUID
from enum import Enum

from . import SBaseModel

AnnotationTaskLabel = ForwardRef('AnnotationTaskLabel')
AnnotationModel = ForwardRef('AnnotationModel')

AnnotationTaskLabelTypes = Literal['bool', 'str', 'int', 'float', 'single', 'multi', 'intext']


class AnnotationTaskLabelChoice(SBaseModel):
    name: str
    hint: str | None = None
    # note, no constraint on value uniqueness; should be checked in frontend
    value: int
    children: list[AnnotationTaskLabel] | None = None


class AnnotationTaskLabel(SBaseModel):
    name: str
    key: str  # note, no check for key uniqueness; should be done in frontend
    hint: str | None = None
    max_repeat: int = 1
    required: bool = True

    kind: AnnotationTaskLabelTypes = 'single'
    # to be used in single or multi, which are dropdown menus
    choices: Optional[list[AnnotationTaskLabelChoice]] = None

    # Only filled when transmitting annotations from/to the gui
    annotation: Optional[AnnotationModel] = None


AnnotationTaskLabelChoice.update_forward_refs()


class FlattenedAnnotationTaskLabel(SBaseModel):
    key: str
    required: bool
    max_repeat: int
    implicit_max_repeat: int
    kind: AnnotationTaskLabelTypes
    choices: list[int] | None
    parent_label: str | None


class AnnotationTaskModel(SBaseModel):
    """
    Corresponds to db.models.annotations.AnnotationTask

    AnnotationTask defines the annotation scheme for a particular project.
    Each project may have multiple AnnotationTasks,
    but projects cannot share the same task. In case they are technically the same,
    the user would have to create a new copy of that task for a different project.

    The actual annotation scheme is defined as a list of labels (see schemas.annotations.AnnotationTaskLabel).
    The other fields pose as meta-data.
    """

    # Unique identifier for this task.
    annotation_task_id: str | UUID | None = None

    # Reference to the project this AnnotationTask belongs to.
    project_id: str | UUID | None = None

    # A short descriptive title / name for this AnnotationTask.
    # This may be displayed to the annotators.
    name: str

    # An (optional) slightly longer description of the AnnotationTask.
    # This may be displayed to the annotators as an instruction or background information.
    # Text should be formatted using Markdown.
    description: str | None = None

    # The definition of the annotation scheme for this AnnotationTask is stored here.
    # For more information on how an annotation scheme is defined, check out schemas.annotations.AnnotationTaskLabel
    # Note, this is always a list of labels!
    labels: list[AnnotationTaskLabel]


AssignmentScopeBaseConfigTypes = Literal['random']


class AssignmentScopeBaseConfig(SBaseModel):
    config_type: AssignmentScopeBaseConfigTypes
    # list of user ids in the pool
    users: list[str] | list[UUID] | None = None


class AssignmentScopeRandomConfig(AssignmentScopeBaseConfig):
    config_type: AssignmentScopeBaseConfigTypes = 'random'
    num_items: int
    min_assignments_per_item: int
    max_assignments_per_item: int
    num_multi_coded_items: int
    random_seed: int


AssignmentScopeConfig = AssignmentScopeRandomConfig


class AssignmentScopeModel(SBaseModel):
    """
    AssignmentScope can be used to logically group a set of Assignments.
    For example, one may wish to re-use the same AnnotationTask several times within a project
    without copying it each time. It may also be used to logically group different scopes of
    the annotation process, for example to make it clear that different subsets of a dataset
    are to be annotated.
    Logically, this should be viewed as a hierarchical organisation
    AnnotationTask -> [AssignmentScope] -> Assignment -> Annotation
    """
    # Unique identifier for this scope
    assignment_scope_id: str | UUID | None = None
    # The AnnotationTask defining the annotation scheme to be used for this scope
    task_id: str | UUID
    # Date and time when this assignment scope was created
    time_created: datetime | None = None
    # A short descriptive title / name for this scope.
    # This may be displayed to the annotators.
    name: str
    # An (optional) slightly longer description of the scope.
    # This may be displayed to the annotators as refined instruction or background information.
    description: str | None = None
    # Config for the assignment (for reference, optional)
    config: AssignmentScopeConfig | None = None


class AssignmentStatus(Enum):
    FULL = 'FULL'  # This assignment was fully and correctly fulfilled
    PARTIAL = 'PARTIAL'  # This assignment was partially fulfilled
    OPEN = 'OPEN'  # This assignment was not attempted
    INVALID = 'INVALID'  # Something does not comply with the annotation scheme and is thus invalid


class AssignmentModel(SBaseModel):
    """
    Corresponds to db.models.annotations.Assignment

    Assignment is used to request a user/annotator (User) to annotate a particular item (BaseItem) in the database
    following a pre-defined annotation scheme (AnnotationTask).

    Each AnnotationTask will have several Assignments.
    Each User will "receive" several Assignments.
    Each Item may have several Assignments (either in relation to different AnnotationTasks or double-coding).
    The Project is implicit by the AnnotationTask.

    The most common use-cases are:
      * Creating assignments in bulk at random (e.g. 3 users should annotate 50 documents each)
      * Creating assignments one at a time based on a set of rules (e.g. for double-coding, defined order, bias, ...)
      * Creating assignments in small batches or one-by-one in prioritised annotation settings
    """
    # Unique identifier for this assignment
    assignment_id: str | UUID | None = None
    # The AssignmentScope this Assignment should logically be grouped into.
    assignment_scope_id: str | UUID
    # The User (or its ID) this AnnotationTask/Item combination is assigned to
    user_id: str | UUID
    # The Item (or its ID) this assigment refers to.
    item_id: str | UUID
    # The AnnotationTask (or its ID) defining the annotation scheme to be used for this assignment
    task_id: str | UUID
    # The status of this assignment (to be updated with each annotation related to this assignment)
    status: AssignmentStatus
    # The order of assignments within the assignment scope
    order: int | None = None


class AnnotationModel(SBaseModel):
    """
    Corresponds to db.models.annotations.Annotation

    Annotation holds the judgement of a User for a specific Item in the context of an AnnotationTask
    as a response to an Assignment.
    Once an Annotation exists, the Assignment should be considered (partially) resolved.

    Note, that AnnotationTask, User, and Item would be implicit by the Assignment.
    However, for ease of use and in favour of fewer joins, this information is replicated here.

    The Annotation refers to an AnnotationTaskLabel defined in an AnnotationTask, which is referred to by its `key`.
    If the task allows the user to make repeated annotations for the same Label (`key`),
    an offset is defined in `repeat` (e.g. for primary technology is "natural tech", secondary is "forests").

    Furthermore, in-text annotations refer to a substring in the Item text, for which the optional fields
    `text_offset_start` and `text_offset_end` can be used.

    Note, that there is no database constraints on the completeness of an Assignment/AnnotationTask.
    The interface/backend code should be used to make sure, to either not allow partial fulfillment of an
    AnnotationTask or not display an Assignment as complete.
    """
    # Unique identifier for this Annotation
    annotation_id: str | UUID | None = None

    # Date and time when this annotation was created (or last changed)
    time_created: datetime | None = None
    time_updated: datetime | None = None

    # The Assignment this Annotation is responding to.
    assignment_id: str | UUID

    # The User(or its ID) the AnnotationTask/Item combination is assigned to
    user_id: str | UUID

    # The Item (or its ID) this assigment refers to (redundant to implicit information from Assignment)
    item_id: str | UUID

    # The AnnotationTask (or its ID) defining the annotation scheme to be used for this assignment
    # (redundant to implicit information from Assignment)
    task_id: str | UUID

    # Defines which AnnotationTaskLabel.key this Annotation refers to.
    # Note, that there is no correctness constraint, the frontend should make sure to send correct data!!
    key: str

    # In the case of AnnotationTaskLabel.repeats > 1, this field can be used
    # to track primary, secondary,... Annotations for that AnnotationTask/key pair.
    # Count starts at 1, resets with each parent repeat.
    repeat: int = 1

    # Reference to the parent labels' annotation.
    parent: str | UUID | None = None

    # Depending on the AnnotationTaskLabel.kind, one of the following fields should be filled.
    # For single and mixed, the AnnotationTaskLabelChoice.value should be filled in value_int.
    value_bool: bool | None = None
    value_int: int | None = None
    value_float: float | None = None
    value_str: str | None = None

    # When the Annotation does not refer to an entire Item, but a sub-string (in-text annotation)
    # of that Item, the following fields should be set with the respective string offset.
    text_offset_start: int | None = None
    text_offset_stop: int | None = None


AnnotationTaskLabel.update_forward_refs()
