from sqlalchemy import String, ForeignKey, DateTime, func, Float, Integer, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid

from sqlalchemy.orm import mapped_column
from sqlalchemy_json import mutable_json_type

from . import BotAnnotationMetaData
from ..base_class import Base
from .projects import Project
from .annotations import AssignmentScope


class AnnotationQuality(Base):
    """
    Annotation Quality Trackers
    Computing annotator agreements is a little too expensive to do on the fly. Hence, we capture different
    quality metrix in this table; one row per assignment scope and label.
    """

    __tablename__ = 'annotation_quality'
    __table_args__ = (UniqueConstraint('assignment_scope_id', 'bot_annotation_metadata_id', 'label_key', 'label_value', 'user_base', 'user_target'),)

    # Unique identifier for this quality tracker.
    annotation_quality_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False, unique=True, index=True)

    # The project this quality tracker is attached to
    project_id = mapped_column(UUID(as_uuid=True), ForeignKey(Project.project_id, ondelete='CASCADE'), nullable=False, index=True, primary_key=False)

    # The assignment scope this quality tracker is referring to
    assignment_scope_id = mapped_column(
        UUID(as_uuid=True), ForeignKey(AssignmentScope.assignment_scope_id, ondelete='CASCADE'), nullable=False, index=True, primary_key=False
    )

    # The resolution this quality tracker is referring to
    bot_annotation_metadata_id = mapped_column(
        UUID(as_uuid=True), ForeignKey(BotAnnotationMetaData.bot_annotation_metadata_id, ondelete='CASCADE'), nullable=True, index=True, primary_key=False
    )

    # Some metrics are computed for pairs of users,
    # in this case both foreign keys should be set, otherwise both shall be NULL
    user_base = mapped_column(String, nullable=True, index=False, primary_key=False)
    annotations_base = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True), nullable=True, index=False)
    user_target = mapped_column(String, nullable=True, index=False, primary_key=False)
    annotations_target = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True), nullable=True, index=False)

    # The label this quality tracker is referring to
    label_key = mapped_column(String, nullable=True, unique=False, index=False)
    # If set, treating multi-choice or single-choice labels as binary task per choice
    label_value = mapped_column(Integer, nullable=True, unique=False, index=False)

    # Inder-rater-reliability scores
    cohen = mapped_column(Float, nullable=True, unique=False, index=False)
    fleiss = mapped_column(Float, nullable=True, unique=False, index=False)
    randolph = mapped_column(Float, nullable=True, unique=False, index=False)
    krippendorff = mapped_column(Float, nullable=True, unique=False, index=False)
    pearson = mapped_column(Float, nullable=True, unique=False, index=False)
    pearson_p = mapped_column(Float, nullable=True, unique=False, index=False)
    kendall = mapped_column(Float, nullable=True, unique=False, index=False)
    kendall_p = mapped_column(Float, nullable=True, unique=False, index=False)
    spearman = mapped_column(Float, nullable=True, unique=False, index=False)
    spearman_p = mapped_column(Float, nullable=True, unique=False, index=False)

    # Agreement scores for multi-label annotation
    multi_overlap_mean = mapped_column(Float, nullable=True, unique=False, index=False)
    multi_overlap_median = mapped_column(Float, nullable=True, unique=False, index=False)
    multi_overlap_std = mapped_column(Float, nullable=True, unique=False, index=False)

    # Retrieval/classification-like scores
    precision = mapped_column(Float, nullable=True, unique=False, index=False)
    recall = mapped_column(Float, nullable=True, unique=False, index=False)
    f1 = mapped_column(Float, nullable=True, unique=False, index=False)

    # Number of annotated items
    num_items = mapped_column(Float, nullable=True, unique=False, index=False)
    # Number of items with more than one annotation
    num_overlap = mapped_column(Float, nullable=True, unique=False, index=False)
    # Number of items where all users agree
    num_agree = mapped_column(Float, nullable=True, unique=False, index=False)
    # Number of items where not all users agree
    num_disagree = mapped_column(Float, nullable=True, unique=False, index=False)
    # Percentage of (num_agree / num_overlap) * 100
    perc_agree = mapped_column(Float, nullable=True, unique=False, index=False)

    # Date and time when this tracker was created (or last updated)
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    time_updated = mapped_column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
