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


class ResolutionMethod(str, Enum):
    majority = 'majority'
    first = 'first'
    last = 'first'
    trust = 'first'


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
    algorithm: ResolutionMethod
    ignore_hierarchy: bool
    ignore_repeat: bool


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
    assignment_scope_id: str | UUID
    annotation_scheme_id: str | UUID


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
