from __future__ import annotations
from typing import Literal, Optional

from datetime import datetime
from uuid import UUID
from enum import Enum
from pydantic import BaseModel

ConsolidationMethod = Literal['majority', 'first', 'last', 'trust']


class BotMetaConsolidate(BaseModel):
    # the algorithm used to (auto-)resolve conflicting annotations
    algorithm: ConsolidationMethod
    # (optional) which annotation scheme this is based on
    scheme: str | None
    # (optional) label in the scheme this consolidates
    label: str | None
    # (optional) list of assignment scopes labels were taken from for consolidation
    source_scopes: list[str] | None
    # (optional) list of users, who's annotations are excluded
    excluded_users: list[str] | None
    # (optional) dictionary of user UUID -> float weights (i.e. trust in the user for weighted majority votes)
    trust: dict[str, float] | None


BotMeta = BotMetaConsolidate


class BotKind(str, Enum):
    # a bot that produces automatic annotations by classifying data
    CLASSIFICATION = 'CLASSIFICATION'
    # a bot that annotates data based on (a set of) rules
    RULES = 'RULES'
    # the labels are the result of a topic model and represent the dominant topic
    TOPICS = 'TOPICS'
    # represents the consolidated labels, may be auto-generated and/or adjusted by hand
    CONSOLIDATE = 'CONSOLIDATE'
    # label ("manually") assigned by an external script / notebook
    SCRIPT = 'SCRIPT'


class BotAnnotationMetaData(BaseModel):
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
    meta: dict[str, str | int | float | bool] | None = None


class BotAnnotation(BaseModel):
    # Unique identifier for this BotAnnotation
    annotation_id: str | UUID | None = None
    # The AnnotationScheme (or its ID) defining the annotation scheme to be used for this assignment
    # (redundant to implicit information from Assignment)
    bot_annotation_metadata_id: str | UUID
    # Date and time when this annotation was created (or last changed)
    time_created: datetime | None = None
    time_updated: datetime | None = None
    # The Item (or its ID) this assigment refers to
    item_id: str | UUID
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
    # (Optional) Confidence scores / probabilities provided by the underlying model
    confidence: float | None = None
