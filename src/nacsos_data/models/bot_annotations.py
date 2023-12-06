from __future__ import annotations

from typing import Literal

from datetime import datetime
from uuid import UUID
from enum import Enum
from pydantic import BaseModel, ConfigDict
from .annotations import (
    AssignmentStatus,
    AnnotationSchemeInfo,
    FlatLabel,
    Label,
    AnnotationValue,
    ItemAnnotation)
from .users import UserModel

AnnotationFiltersType = dict[str, str | list[str] | int | list[int] | None]

ResolutionMethod = Literal['majority', 'first', 'last', 'trust']


class AnnotationFilters(BaseModel):
    """
    Filter rules for fetching all annotations that match these conditions
    It is up to the user of this function to make sure to provide sensible filters!
    All filters are conjunctive (connected with "AND"); if None, they are not included

    There are no "exclude" filters by design. If needed, they should be simulated in the interface.

    :param scheme_id: if not None: annotation has to be part of this annotation scheme
    :param scope_id: if not None: annotation has to be part of this assignment scope
    :param user_id: if not None: annotation has to be by this user
    :param key: if not None: annotation has to be for this AnnotationSchemeLabel.key (or list/tuple of keys)
    :param repeat: if not None: annotation has to be primary/secondary/...
    """
    model_config = ConfigDict(extra='ignore')

    scheme_id: str
    scope_id: str | list[str] | None = None
    user_id: str | list[str] | None = None
    key: str | list[str] | None = None

    @property
    def user_ids(self) -> list[str] | None:
        if self.user_id is None:
            return None
        return [self.user_id] if type(self.user_id) is str else self.user_id  # type: ignore[return-value]

    @property
    def scope_ids(self) -> list[str] | None:
        if self.scope_id is None:
            return None
        return [self.scope_id] if type(self.scope_id) is str else self.scope_id  # type: ignore[return-value]

    @property
    def keys(self) -> list[str] | None:
        if self.key is None:
            return None
        return [self.key] if type(self.key) is str else self.key  # type: ignore[return-value]


class SnapshotEntry(AnnotationValue):
    # values are inherited
    order_key: str  # see `ResolutionOrdering.key` (row in matrix)
    path_key: str  # see `FlatLabel.path_key` (column in matrix)
    item_id: str  # id of the item
    anno_id: str  # related `annotation.annotation_id`
    user_id: str  # id of the user


class ResolutionSnapshotEntry(BaseModel):
    order_key: str  # see `ResolutionOrdering.key` (row in matrix)
    path_key: str  # see `FlatLabel.path_key` (column in matrix)
    ba_id: str  # related `bot_annotation.bot_annotation_id`


class BotMetaResolveBase(BaseModel):
    # the algorithm used to (auto-)resolve conflicting annotations
    algorithm: ResolutionMethod
    # defines the "scope" of labels to include for the resolver
    filters: AnnotationFilters
    ignore_hierarchy: bool
    ignore_repeat: bool
    # (optional) dictionary of user UUID -> float weights (i.e. trust in the user for weighted majority votes)
    trust: dict[str, float] | None = None


class BotMetaResolve(BotMetaResolveBase):
    # snapshot of the annotations and bot_annotations used to resolve this
    snapshot: list[SnapshotEntry]
    resolutions: list[ResolutionSnapshotEntry]


BotMeta = BotMetaResolve


class BotKind(str, Enum):
    # a bot that produces automatic annotations by classifying data
    CLASSIFICATION = 'CLASSIFICATION'
    # a bot that annotates data based on (a set of) rules
    RULES = 'RULES'
    # the labels are the result of a topic model and represent the dominant topic
    TOPICS = 'TOPICS'
    # represents the consolidated labels, may be auto-generated and/or adjusted by hand
    RESOLVE = 'RESOLVE'
    # label ("manually") assigned by an external script / notebook
    SCRIPT = 'SCRIPT'


class BotAnnotationMetaDataBaseModel(BaseModel):
    bot_annotation_metadata_id: str | UUID | None = None
    # A short descriptive title / name for this bot
    name: str
    # Indicator for the kind of bot annotation
    kind: BotKind
    # Reference to a project
    project_id: str | UUID
    # Date and time when this meta entry was created (or last changed)
    time_created: datetime | None = None
    time_updated: datetime | None = None
    # (Optional) reference to an assignment scope
    assignment_scope_id: str | UUID | None = None
    # (Optional) reference to an annotation scheme used here
    annotation_scheme_id: str | UUID | None = None


class BotAnnotationResolution(BotAnnotationMetaDataBaseModel):
    # Additional information for this Bot for future reference
    meta: BotMetaResolve


class BotAnnotationMetaDataModel(BotAnnotationMetaDataBaseModel):
    # Additional information for this Bot for future reference
    meta: BotMeta | None = None


class BotAnnotationModel(AnnotationValue):
    # Unique identifier for this BotAnnotation
    bot_annotation_id: str | UUID | None = None
    # The AnnotationScheme (or its ID) defining the annotation scheme to be used for this assignment
    # (redundant to implicit information from Assignment)
    bot_annotation_metadata_id: str | UUID | None = None
    # Date and time when this annotation was created (or last changed)
    time_created: datetime | None = None
    time_updated: datetime | None = None
    # The Item (or its ID) this assigment refers to
    item_id: str | UUID
    # parent BotAnnotation
    parent: str | UUID | None = None
    # (Optional) Defines which AnnotationSchemeLabel.key this Annotation refers to.
    key: str | None = None
    # Indicates primary/secondary label, but can also be used to store multiple predictions (e.g. the top five topics)
    # Default should always be 1.
    repeat: int = 1
    # (Optional) Indicate the order of this annotation (e.g. from assignment order);
    # Only assumed to be valid within each BotAnnotationMetaData scope
    order: int | None = None
    # (Optional) Confidence scores / probabilities provided by the underlying model
    confidence: float | None = None


class BotItemAnnotation(BotAnnotationModel):
    path: list[Label]
    old: AnnotationValue | None = None


class DehydratedAssignment(BaseModel):
    assignment_id: str
    user_id: str
    item_id: str
    username: str
    status: AssignmentStatus
    order: int


class ResolutionOrdering(BaseModel):
    model_config = ConfigDict(extra='ignore')
    identifier: int
    first_occurrence: int
    item_id: str
    scope_id: str
    key: str


class OrderingEntry(ResolutionOrdering):
    assignments: list[DehydratedAssignment]


class ResolutionStatus(str, Enum):
    NEW = 'NEW'
    CHANGED = 'CHANGED'
    UNCHANGED = 'UNCHANGED'


class ResolutionUserEntry(BaseModel):
    assignment: DehydratedAssignment | None = None
    annotation: ItemAnnotation | None = None
    status: ResolutionStatus = ResolutionStatus.UNCHANGED


class ResolutionCell(BaseModel):
    labels: dict[str, list[ResolutionUserEntry]]  # username: ResolutionUserEntry[]
    resolution: BotAnnotationModel
    status: ResolutionStatus = ResolutionStatus.UNCHANGED


AssignmentMap = dict[str, tuple[DehydratedAssignment, OrderingEntry]]
ResolutionMatrix = dict[str, dict[str, ResolutionCell]]


class ResolutionProposal(BaseModel):
    scheme_info: AnnotationSchemeInfo
    labels: list[FlatLabel]
    annotators: list[UserModel]
    ordering: list[ResolutionOrdering]
    matrix: ResolutionMatrix
