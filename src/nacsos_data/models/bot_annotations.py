from __future__ import annotations
from typing import Literal

from datetime import datetime
from uuid import UUID
from enum import Enum
from pydantic import BaseModel
from .annotations import AnnotationModel
from .users import UserModel

AnnotationFiltersType = dict[str, str | tuple[str] | int | tuple[int] | None]


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
    scheme_id: str
    scope_id: str | list[str] | None = None
    user_id: str | list[str] | None = None
    key: str | list[str] | None = None
    repeat: int | list[int] | None = None


class Label(BaseModel):
    """
    Convenience type (corresponding to internal type in db annotation_label).
    For Annotation or BotAnnotation, this is the combination of their respective key, repeat value.

    Mainly used during resolving annotations.
    """
    key: str
    repeat: int


class _AnnotationCollection(BaseModel):
    scheme_id: str
    labels: list[list[Label]]
    # key: item_id
    annotations: dict[str, tuple[list[Label], list[AnnotationModel]]]


class AnnotationCollectionDB(_AnnotationCollection):
    annotators: list[str]


class AnnotationCollection(_AnnotationCollection):
    annotators: list[UserModel]


ResolutionMethod = Literal['majority', 'first', 'last', 'trust']


class BotMetaResolve(BaseModel):
    # the algorithm used to (auto-)resolve conflicting annotations
    algorithm: ResolutionMethod
    # defines the "scope" of labels to include for the resolver
    filters: AnnotationFilters
    ignore_hierarchy: bool
    ignore_repeat: bool
    # (optional) dictionary of user UUID -> float weights (i.e. trust in the user for weighted majority votes)
    trust: dict[str, float] | None = None
    # snapshot of the annotations used to resolve this
    collection: AnnotationCollectionDB


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


class BotAnnotationMetaDataModel(BaseModel):
    bot_annotation_metadata_id: str | UUID | None = None
    # A short descriptive title / name for this bot
    name: str
    # Indicator for the kind of bot annotation
    kind: BotKind
    # Reference to a project
    project_id: str | UUID
    # (Optional) reference to an annotation scope
    annotation_scope_id: str | UUID | None = None
    # (Optional) reference to an annotation scheme used here
    annotation_scheme_id: str | UUID | None = None
    # Additional information for this Bot for future reference
    meta: BotMeta | None = None


class BotAnnotationModel(BaseModel):
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
    # Exactly one of the following fields should be filled.
    # Contains the value for this annotation (e.g. numbered class from annotation_scheme)
    value_bool: bool | None = None
    value_int: int | None = None
    value_float: float | None = None
    value_str: str | None = None
    multi_int: list[int] | None = None
    # (Optional) Confidence scores / probabilities provided by the underlying model
    confidence: float | None = None
