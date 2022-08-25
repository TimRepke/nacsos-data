from sqlalchemy import Integer, String, ForeignKey, Boolean, Float, DateTime, Column, \
    UniqueConstraint, Identity, Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy_json import mutable_json_type
import uuid

from ...models.annotations import AssignmentStatus
from ...db.base_class import Base

from .projects import Project
from .users import User
from .items import Item


class AnnotationScheme(Base):
    """
    AnnotationScheme defines the annotation scheme for a particular project.
    Each project may have multiple AnnotationSchemes,
    but projects cannot share the same scheme. In case they are technically the same,
    the user would have to create a new copy of that scheme for a different project.

    The actual annotation scheme is defined as a list of labels (see schemas.annotations.AnnotationSchemeLabel).
    The other fields pose as meta-data.
    """
    __tablename__ = 'annotation_scheme'

    # Unique identifier for this scheme.
    annotation_scheme_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                                  nullable=False, unique=True, index=True)

    # Reference to the project this AnnotationScheme belongs to.
    project_id = Column(UUID(as_uuid=True),
                        ForeignKey(Project.project_id),  # type: ignore[misc] # FIXME
                        nullable=False)

    # A short descriptive title / name for this AnnotationScheme.
    # This may be displayed to the annotators.
    name = Column(String, nullable=False)

    # An (optional) slightly longer description of the AnnotationScheme.
    # This may be displayed to the annotators as an instruction or background information.
    # Text should be formatted using Markdown.
    description = Column(String, nullable=True)

    # The definition of the annotation scheme for this AnnotationScheme is stored here.
    # For more information on how an annotation scheme is defined, check out schemas.annotations.AnnotationSchemeLabel
    # Note, this is always a list of labels!
    labels = Column(mutable_json_type(dbtype=JSONB, nested=True))  # type: ignore[misc] # FIXME

    # reference to the associated assignment scopes
    assignment_scopes = relationship('AssignmentScope', cascade='all, delete')  # type: ignore[misc] # FIXME


class AssignmentScope(Base):
    """
    AssignmentScope can be used to logically group a set of Assignments.
    For example, one may wish to re-use the same AnnotationScheme several times within a project
    without copying it each time. It may also be used to logically group different scopes of
    the annotation process, for example to make it clear that different subsets of a dataset
    are to be annotated.
    Logically, this should be viewed as a hierarchical organisation
    AnnotationScheme -> [AssignmentScope] -> Assignment -> Annotation
    """
    __tablename__ = 'assignment_scope'

    # Unique identifier for this scope
    assignment_scope_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                                 nullable=False, unique=True, index=True)

    # The AnnotationScheme defining the annotation scheme to be used for this scope
    annotation_scheme_id = Column(UUID(as_uuid=True),
                                  ForeignKey(AnnotationScheme.annotation_scheme_id),  # type: ignore[misc] # FIXME
                                  nullable=False, index=True)

    # Date and time when this assignment scope was created
    time_created = Column(DateTime(timezone=True), server_default=func.now())

    # A short descriptive title / name for this scope.
    # This may be displayed to the annotators.
    name = Column(String, nullable=False)

    # An (optional) slightly longer description of the scope.
    # This may be displayed to the annotators as refined instruction or background information.
    description = Column(String, nullable=True)

    # Stores the config parameters used in creating the assignments for future reference
    config = Column(mutable_json_type(dbtype=JSONB, nested=True), nullable=True)  # type: ignore[misc] # FIXME

    # reference to the associated assignments
    assignments = relationship('Assignment', cascade='all, delete')  # type: ignore[misc] # FIXME


class Assignment(Base):
    """
    Assignment is used to request a user/annotator (User) to annotate a particular item (BaseItem) in the database
    following a pre-defined annotation scheme (AnnotationScheme).

    Each AnnotationScheme will have several Assignments.
    Each User will "receive" several Assignments.
    Each Item may have several Assignments (either in relation to different AnnotationSchemes or double-coding).
    The Project is implicit by the AnnotationScheme.

    The most common use-cases are:
      * Creating assignments in bulk at random (e.g. 3 users should annotate 50 documents each)
      * Creating assignments one at a time based on a set of rules (e.g. for double-coding, defined order, bias, ...)
      * Creating assignments in small batches or one-by-one in prioritised annotation settings
    """
    __tablename__ = 'assignment'

    # Unique identifier for this assignment
    assignment_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                           nullable=False, unique=True, index=True)
    # The AssignmentScope this Assignment should logically be grouped into.
    assignment_scope_id = Column(UUID(as_uuid=True),
                                 ForeignKey(AssignmentScope.assignment_scope_id),  # type: ignore[misc] # FIXME
                                 nullable=False, index=True)
    # The User the AnnotationScheme/Item combination is assigned to
    user_id = Column(UUID(as_uuid=True),
                     ForeignKey(User.user_id),  # type: ignore[misc] # FIXME
                     nullable=False, index=True)

    # The Item this assigment refers to.
    item_id = Column(UUID(as_uuid=True),
                     ForeignKey(Item.item_id),  # type: ignore[misc] # FIXME
                     nullable=False, index=True)

    # The AnnotationScheme defining the annotation scheme to be used for this assignment
    annotation_scheme_id = Column(UUID(as_uuid=True),
                                  ForeignKey(AnnotationScheme.annotation_scheme_id),  # type: ignore[misc] # FIXME
                                  nullable=False, index=True)

    # The status of this assignment (to be updated with each annotation related to this assignment)
    status = Column(SAEnum(AssignmentStatus), nullable=False, server_default='OPEN')

    # The order of assignments within the assignment scope
    order = Column(Integer, Identity(always=False))

    # TODO figure out how to nicely resolve in-text annotations here


class Annotation(Base):
    """
    Annotation holds the judgement of a User for a specific Item in the context of an AnnotationScheme
    as a response to an Assignment.
    Once an Annotation exists, the Assignment should be considered (partially) resolved.

    Note, that AnnotationScheme, User, and Item would be implicit by the Assignment.
    However, for ease of use and in favour of fewer joins, this information is replicated here.

    The Annotation refers to an AnnotationSchemeLabel defined in an AnnotationScheme, which is referred to by its `key`.
    If the scheme allows the user to make repeated annotations for the same Label (`key`),
    an offset is defined in `repeat` (e.g. for primary technology is "natural tech", secondary is "forests").

    Furthermore, in-text annotations refer to a substring in the Item text, for which the optional fields
    `text_offset_start` and `text_offset_end` can be used.

    Note, that there is no database constraints on the completeness of an Assignment/AnnotationScheme.
    The interface/backend code should be used to make sure, to either not allow partial fulfillment of an
    AnnotationScheme or not display an Assignment as complete.
    """
    __tablename__ = 'annotation'
    __table_args__ = (
        UniqueConstraint('assignment_id', 'key', 'parent', 'repeat'),
    )

    # Unique identifier for this Annotation
    annotation_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                           nullable=False, unique=True, index=True)

    # Date and time when this annotation was created (or last changed)
    time_created = Column(DateTime(timezone=True), server_default=func.now())
    time_updated = Column(DateTime(timezone=True), onupdate=func.now())

    # The Assignment this Annotation is responding to.
    assignment_id = Column(UUID(as_uuid=True),
                           ForeignKey(Assignment.assignment_id),  # type: ignore[misc] # FIXME
                           nullable=False, index=True)

    # The User the AnnotationScheme/Item combination is assigned to (redundant to implicit information from Assignment)
    user_id = Column(UUID(as_uuid=True),
                     ForeignKey(User.user_id),  # type: ignore[misc] # FIXME
                     nullable=False, index=True)

    # The Item this assigment refers to (redundant to implicit information from Assignment)
    item_id = Column(UUID(as_uuid=True),
                     ForeignKey(Item.item_id),  # type: ignore[misc] #FIXME
                     nullable=False, index=True)

    # The AnnotationScheme defining the annotation scheme to be used for this assignment
    # (redundant to implicit information from Assignment)
    annotation_scheme_id = Column(UUID(as_uuid=True),
                                  ForeignKey(AnnotationScheme.annotation_scheme_id),  # type: ignore[misc] # FIXME
                                  nullable=False, index=True)

    # Defines which AnnotationSchemeLabel.key this Annotation refers to.
    # Note, that there is no correctness constraint, the frontend should make sure to send correct data!!
    key = Column(String, nullable=False)

    # In the case of AnnotationSchemeLabel.repeats > 1, this field can be used
    # to track primary, secondary,... Annotations for that AnnotationScheme/key pair.
    # Default should always be 1.
    repeat = Column(Integer, nullable=False, default=1)

    # Reference to the parent labels' annotation.
    parent = Column(UUID(as_uuid=True),
                    ForeignKey('annotation.annotation_id'),  # type: ignore[arg-type] # FIXME
                    nullable=True, index=True)

    # Depending on the AnnotationSchemeLabel.kind, one of the following fields should be filled.
    # For single and mixed, the AnnotationSchemeLabelChoice.value should be filled in value_int.
    value_bool = Column(Boolean, nullable=True)
    value_int = Column(Integer, nullable=True)
    value_float = Column(Float, nullable=True)
    value_str = Column(String, nullable=True)

    # When the Annotation does not refer to an entire Item, but a sub-string (in-text annotation)
    # of that Item, the following fields should be set with the respective string offset.
    text_offset_start = Column(Integer, nullable=True)
    text_offset_stop = Column(Integer, nullable=True)

    sub_annotations = relationship('Annotation', cascade='all, delete')  # type: ignore[misc] # FIXME

    # TODO: Figure out a way to allow automated methods (e.g. classifiers) to utilise this
    #       table to annotate data as well. Creating loads of dummy users and assignments
    #       would be infeasible, esp. considering there might be loads of different (parametrised) models.
