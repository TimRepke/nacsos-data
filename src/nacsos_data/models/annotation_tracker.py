import uuid
from datetime import datetime

from pydantic import BaseModel

H0Series = list[tuple[int, float]]


class AnnotationTrackerModel(BaseModel):
    """
    Tracker for annotation statistics.
    This includes the latest stopping criterion (buscar) metrics and more.

    You may have more than one tracker per project, for example for keeping track of different progresses.
    """
    # Unique identifier for this tracker.
    annotation_tracking_id: uuid.UUID | str | None = None
    # The project this tracker is attached to
    project_id: uuid.UUID | str
    # Descriptive name for this tracker
    name: str
    # String describing which (combination of) labels reflect inclusion criteria
    inclusion_rule: str
    # If False, inclusion_rule is fulfilled if any annotation matches, otherwise majority vote counts
    majority: bool
    # Number of items (usually this will be the number of items in the number of items in the project)
    n_items_total: int
    # Recall target (parameter for the BUSCAR metric)
    recall_target: float
    # Include resolved labels from these resolutions or assignment scopes
    #   -> ForeignKey(BotAnnotationMetaData.bot_annotation_metadata_id)
    #   -> ForeignKey(AssignmentScope.assignment_scope_id)
    source_ids: list[uuid.UUID] | list[str] | None = None
    # Sequence of labels (usually {0, 1}) these metrics are based on (list of lists matching labels per source_id)
    labels: list[list[int]] | None = None
    # Recall (after each annotation)
    recall: list[float] | None = None
    # list[tuple[int, float]] of the BUSCAR metric (stopping criterion)
    buscar: H0Series | None = None
    # Date and time when this tracker was created (or last updated)
    time_created: datetime | None = None
    time_updated: datetime | None = None