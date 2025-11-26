import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated

from nacsos_data.models.nql import NQLFilter


class _MLConfig(BaseModel):
    model_config = ConfigDict(extra='ignore')


class _BERTModel(_MLConfig):
    max_len: int = 512
    train_split: float = 0.9
    n_epochs: int = 3
    batch_size_predict: int = 50
    batch_size_train: int = 16
    batch_size_eval: int = 50
    warmup_steps: int = 400
    weight_decay: float = 0.01
    logging_steps: int = 10
    eval_strategy: str = 'steps'
    eval_steps: int = 50


class SciBERTModel(_BERTModel):
    conf: Literal['SCIBERT'] = 'SCIBERT'
    model: str = 'allenai/scibert_scivocab_uncased'


class ClimateBERTModel(_BERTModel):
    conf: Literal['CLIMBERT'] = 'CLIMBERT'
    model: str = 'climatebert/distilroberta-base-climate-f'


class _TfIdf(_MLConfig):
    stop_words: Literal['english'] | list[str] | None = 'english'
    ngram_range: tuple[int, int] = (1, 1)
    max_df: float | int = 1.0
    min_df: float | int = 1
    max_features: int | None = None


class SVMModel(_TfIdf):
    conf: Literal['SVM'] = 'SVM'
    # see https://scikit-learn.org/stable/modules/generated/sklearn.svm.SVC.html#sklearn.svm.SVC
    C: float = 1.0
    kernel: Literal['linear', 'poly', 'rbf', 'sigmoid', 'precomputed'] = 'rbf'
    degree: int = 3


class RegressionModel(_TfIdf):
    conf: Literal['REG'] = 'REG'


PriorityModelConfig = Annotated[SciBERTModel | ClimateBERTModel | RegressionModel | SVMModel, Field(discriminator='conf')]


class _PriorityModel(BaseModel):
    # Unique identifier for this task.
    priority_id: str | uuid.UUID | None = None

    # Project this task is attached to
    project_id: str | uuid.UUID | None = None

    # Name of this setup for reference
    name: str | None = None

    # Timestamps for when setup was created, training started, predictions are ready, and predictions were used in assignment
    time_created: datetime | None = None
    time_started: datetime | None = None
    time_ready: datetime | None = None
    time_assigned: datetime | None = None


class DehydratedPriorityModel(_PriorityModel):
    # Length of the `prioritised_ids` array
    num_prioritised: int | None = None


class PriorityModel(_PriorityModel):
    # ForeignKey(BotAnnotationMetaData.bot_annotation_metadata_id or AssignmentScope.assignment_scope_id)
    source_scopes: list[str] | list[uuid.UUID] | None = None

    # NQL Filter for the dataset
    # Filter for which items to use for prediction AND training (labels are not an outer join!)
    nql: str | None = None
    nql_parsed: NQLFilter | None = None

    # Rule for inclusion definition from columns
    incl_rule: str | None = None
    # Column name to write rule result to
    incl_field: str | None = None
    # Column name to write model predictions to
    incl_pred_field: str | None = None

    # Percentage of overall data to use in training
    train_split: float | None = None
    # Number of predictions to keep
    n_predictions: int | None = None

    # JSON dump of `PriorityModelConfig`
    config: PriorityModelConfig | None = None

    # ForeignKey(Item.item_id)
    prioritised_ids: list[str] | list[uuid.UUID] | None = None
