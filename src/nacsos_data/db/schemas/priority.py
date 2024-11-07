import uuid

from sqlalchemy import String, ForeignKey, DateTime, func, Float, Integer
from sqlalchemy.orm import mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from ..base_class import Base
from .projects import Project


class Priority(Base):
    __tablename__ = 'priorities'

    # Unique identifier for this task.
    priority_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                                nullable=False, unique=True, index=True)

    # Project this task is attached to
    project_id = mapped_column(UUID(as_uuid=True),
                               ForeignKey(Project.project_id, ondelete='CASCADE'),
                               nullable=False, index=True, primary_key=False)

    # Name of this setup for reference
    name = mapped_column(String, nullable=False, unique=False, index=False)

    # Timestamps for when setup was created, training started, predictions are ready, and predictions were used in assignment
    time_created = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    time_started = mapped_column(DateTime(timezone=True), nullable=True)
    time_ready = mapped_column(DateTime(timezone=True), nullable=True)
    time_assigned = mapped_column(DateTime(timezone=True), nullable=True)

    # ForeignKey(BotAnnotationMetaData.bot_annotation_metadata_id or AssignmentScope.assignment_scope_id)
    source_scopes = mapped_column(ARRAY(UUID(as_uuid=True)), nullable=False, index=False)

    # NQL Filter for the dataset
    # Filter for which items to use for prediction AND training (labels are not an outer join!)
    nql = mapped_column(String, nullable=True, unique=False, index=False)

    # Rule for inclusion definition from columns
    incl_rule = mapped_column(String, nullable=False, unique=False, index=False)
    # Column name to write rule result to
    incl_field = mapped_column(String, default='incl', nullable=False, unique=False, index=False)
    # Column name to write model predictions to
    incl_pred_field = mapped_column(String, default='pred|incl', nullable=False, unique=False, index=False)

    # Percentage of overall data to use in training
    train_split = mapped_column(Float, nullable=False, unique=False, index=False)
    # Number of predictions to keep
    n_predictions = mapped_column(Integer, nullable=False, unique=False, index=False)

    # JSON dump of `PriorityModelConfig`
    config = mapped_column(JSONB, nullable=False, index=False)

    # ForeignKey(Item.item_id)
    prioritised_ids = mapped_column(ARRAY(UUID(as_uuid=True)), nullable=True, index=False)
