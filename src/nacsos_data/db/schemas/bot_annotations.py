import uuid
from sqlalchemy import \
    Integer, \
    Float, \
    String, \
    Boolean, \
    DateTime, \
    Enum as SAEnum, \
    ForeignKey, \
    UniqueConstraint, \
    CheckConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column, relationship, Relationship
from sqlalchemy.dialects.postgresql import JSONB, UUID, ARRAY
from sqlalchemy_json import mutable_json_type

from ...models.bot_annotations import BotKind, BotMeta
from ...db.base_class import Base
from .projects import Project
from .items.base import Item
from . import AnnotationScheme, AssignmentScope


class BotAnnotationMetaData(Base):
    """
    In order to keep track of where automatic annotations came from, we record some
    meta-data here. All BotAnnotations reference to this.
    This can also be considered as a "scope" to group annotations, i.e. there might be
    multiple BotAnnotationMetaData for a classification model applied in different contexts.
    """
    __tablename__ = 'bot_annotation_metadata'

    # Unique identifier for this Bot
    bot_annotation_metadata_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                                               nullable=False, unique=True, index=True)
    # A short descriptive title / name for this bot
    name = mapped_column(String, nullable=False)
    # Indicator for the kind of bot annotation
    kind = mapped_column(SAEnum(BotKind), nullable=False)
    # Reference to a project
    project_id = mapped_column(UUID(as_uuid=True),
                               ForeignKey(Project.project_id),
                               nullable=False, index=True)

    # Date and time when this meta entry was created (or last changed)
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now())
    time_updated = mapped_column(DateTime(timezone=True), onupdate=func.now())

    # (Optional) reference to an assignment scope
    annotation_scope_id = mapped_column(UUID(as_uuid=True),  # FIXME: fix name to assignment_scope_id
                                        ForeignKey(AssignmentScope.assignment_scope_id),
                                        nullable=True, index=True)
    # (Optional) reference to an annotation scheme used here
    annotation_scheme_id = mapped_column(UUID(as_uuid=True),
                                         ForeignKey(AnnotationScheme.annotation_scheme_id),
                                         nullable=True, index=True)
    # Additional information for this Bot for future reference
    meta: Mapped[BotMeta] = mapped_column(mutable_json_type(dbtype=JSONB, nested=True),
                                          nullable=True)

    # reference to the associated bot_annotations
    bot_annotations: Relationship['BotAnnotation'] = relationship('BotAnnotation', cascade='all, delete')


class BotAnnotation(Base):
    """

    """
    __tablename__ = 'bot_annotation'
    __table_args__ = (
        UniqueConstraint('bot_annotation_metadata_id', 'item_id', 'parent', 'key', 'repeat'),
        CheckConstraint('num_nonnulls(value_bool, value_int, value_float, value_str, multi_int) = 1',
                        name='bot_annotation_has_value'),
    )

    # Unique identifier for this BotAnnotation
    bot_annotation_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                                      nullable=False, unique=True, index=True)
    bot_annotation_metadata_id = mapped_column(UUID(as_uuid=True),
                                               ForeignKey(BotAnnotationMetaData.bot_annotation_metadata_id),
                                               nullable=False, index=True)

    # Date and time when this annotation was created (or last changed)
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now())
    time_updated = mapped_column(DateTime(timezone=True), onupdate=func.now())

    # The Item this assigment refers to
    item_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(Item.item_id),
                            nullable=False, index=True)

    # Reference to the parent labels' annotation.
    parent = mapped_column(UUID(as_uuid=True),
                           ForeignKey('bot_annotation.bot_annotation_id'),
                           nullable=True, index=True)

    # (Optional) Defines which AnnotationSchemeLabel.key this Annotation refers to.
    key = mapped_column(String, nullable=True)

    # Indicates primary/secondary label, but can also be used to store multiple predictions (e.g. the top five topics)
    # Default should always be 1.
    repeat = mapped_column(Integer, nullable=False, default=1)

    # Exactly one of the following fields should be filled.
    # Contains the value for this annotation (e.g. numbered class from annotation_scheme)
    value_bool = mapped_column(Boolean, nullable=True)
    value_int = mapped_column(Integer, nullable=True)
    value_float = mapped_column(Float, nullable=True)
    value_str = mapped_column(String, nullable=True)
    multi_int = mapped_column(ARRAY(Integer), nullable=True)

    # (Optional) Confidence scores / probabilities provided by the underlying model
    confidence = mapped_column(Float, nullable=True)
