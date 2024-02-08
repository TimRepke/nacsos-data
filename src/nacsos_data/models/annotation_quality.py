import uuid
from datetime import datetime

from pydantic import BaseModel


class AnnotationQualityModel(BaseModel):
    """
    Annotation Quality Trackers
    Computing annotator agreements is a little too expensive to do on the fly. Hence, we capture different
    quality metrics in this table; one row per assignment scope and label.
    """

    # Unique identifier for this quality tracker.
    annotation_quality_id: uuid.UUID | str | None = None

    # The project this quality tracker is attached to
    project_id: uuid.UUID | str | None = None

    # The assignment scope this quality tracker is referring to
    assignment_scope_id: uuid.UUID | str | None = None

    # Some metrics are computed for pairs of users,
    # in this case both foreign keys should be set, otherwise both shall be NULL
    user_base: uuid.UUID | str | None = None
    annotations_base: list[bool | None] | list[int] | list[int | None] | list[list[int] | None] | None = None
    user_target: uuid.UUID | str | None = None
    annotations_target: list[bool | None] | list[int] | list[int | None] | list[list[int] | None] | None = None

    # The label this quality tracker is referring to
    label_key: str | None = None
    # If set, treating multi-choice or single-choice labels as binary task per choice
    label_value: int | None = None

    # Inder-rater-reliability scores
    cohen: float | None = None
    fleiss: float | None = None
    randolph: float | None = None
    krippendorff: float | None = None
    pearson: float | None = None
    pearson_p: float | None = None
    kendall: float | None = None
    kendall_p: float | None = None
    spearman: float | None = None
    spearman_p: float | None = None

    precision: float | None = None
    recall: float | None = None
    f1: float | None = None

    # Agreement scores for multi-label annotation
    multi_overlap_mean: float | None = None
    multi_overlap_median: float | None = None
    multi_overlap_std: float | None = None

    # Number of annotated items
    num_items: int | None = None
    # Number of items with more than one annotation
    num_overlap: int | None = None
    # Number of items where all users agree
    num_agree: int | None = None
    # Number of items where not all users agree
    num_disagree: int | None = None
    # Percentage of (num_agree / num_overlap) * 100
    perc_agree: float | None = None

    # Date and time when this tracker was created (or last updated)
    time_created: datetime | None = None
    time_updated: datetime | None = None
