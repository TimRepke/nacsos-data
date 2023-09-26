from sqlalchemy import String, ForeignKey, DateTime, func, ARRAY, Float, Integer, Boolean
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid

from sqlalchemy.orm import mapped_column
from sqlalchemy_json import mutable_json_type

from ..base_class import Base
from .projects import Project


class AnnotationTracker(Base):
    """
    Tracker for annotation statistics.
    This includes the latest stopping criterion (buscar) metrics and more.

    You may have more than one tracker per project, for example for keeping track of different progresses.
    """
    __tablename__ = 'annotation_tracker'

    # Unique identifier for this tracker.
    annotation_tracking_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                                           nullable=False, unique=True, index=True)
    # The project this tracker is attached to
    project_id = mapped_column(UUID(as_uuid=True),
                               ForeignKey(Project.project_id, ondelete='CASCADE'),
                               nullable=False, index=True, primary_key=False)
    # Descriptive name for this tracker
    name = mapped_column(String, nullable=False, unique=False, index=False)
    # String describing which (combination of) labels reflect inclusion criteria
    inclusion_rule = mapped_column(String, nullable=False, unique=False, index=False)
    # If False, inclusion_rule is fulfilled if any annotation matches, otherwise majority vote counts
    majority = mapped_column(Boolean, nullable=False, unique=False, index=False)
    # Number of items (usually this will be the number of items in the number of items in the project)
    n_items_total = mapped_column(Integer, nullable=False, unique=False, index=False)
    # Recall target (parameter for the BUSCAR metric)
    recall_target = mapped_column(Float, nullable=False, unique=False, index=False)
    # Include resolved labels from these resolutions or assignment scopes
    #   -> ForeignKey(BotAnnotationMetaData.bot_annotation_metadata_id)
    #   -> ForeignKey(AssignmentScope.assignment_scope_id)
    source_ids = mapped_column(ARRAY(UUID(as_uuid=True)), nullable=True, index=False)
    # Sequence of labels {0, 1} these metrics are based on (so we don't have to run complex queries on every lookup)
    labels = mapped_column(ARRAY(Integer), nullable=True, index=False)
    # Recall (after each annotation)
    recall = mapped_column(ARRAY(Float), nullable=True, index=False)
    # list[tuple[int, float]] of the BUSCAR metric (stopping criterion)
    buscar = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True))

    # Date and time when this tracker was created (or last updated)
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    time_updated = mapped_column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
