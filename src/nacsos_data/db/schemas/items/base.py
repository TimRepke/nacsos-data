import uuid
from typing import TYPE_CHECKING

from sqlalchemy import String, ForeignKey, Enum as SAEnum, DateTime
from sqlalchemy.orm import relationship, mapped_column, Mapped
from sqlalchemy.dialects.postgresql import UUID

from ...base_class import Base
from ..projects import Project
from ..imports import m2m_import_item_table, Import
from . import ItemType

if TYPE_CHECKING:
    from ..annotations import Annotation, Assignment
    from ..enhancements import Enhancement


class Item(Base):
    """
    Item is the abstract parent class/schema for all data stored on the platform.
    It only contains the bare minimum information (text, project, type).
    Sub-schemas provide additional meta-data based on the `Item.type`

    For querying and polymorphic querying, see the SQLAlchemy guide:
    https://docs.sqlalchemy.org/en/20/orm/queryguide/inheritance.html#loading-joined-inheritance
    """

    __tablename__ = 'item'

    # Unique identifier for this Item.
    item_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False, unique=True, index=True)

    # reference to the project this item belongs to
    # see https://apsis.mcc-berlin.net/nacsos-docs/dev/schema/20_data/
    project_id = mapped_column(UUID(as_uuid=True), ForeignKey(Project.project_id, ondelete='cascade'), nullable=False, index=True, primary_key=False)

    # The text for this item
    text = mapped_column(String, nullable=True)

    # Discriminator for figuring out which subclass to load for this item for more details
    # Note, that all items within a project must have the same type (thus `Item.type` == `Project.type`)
    type = mapped_column(SAEnum(ItemType), nullable=False)

    # Date when this item was edited; if NULL, considered unedited; if not NULL, consider to never auto-update
    time_edited = mapped_column(DateTime(timezone=True), default=None, nullable=True)

    imports: Mapped[list[Import]] = relationship(
        'Import',
        secondary=m2m_import_item_table,
        back_populates='items',
    )

    annotations: Mapped[list['Annotation']] = relationship('Annotation')
    assignments: Mapped[list['Assignment']] = relationship('Assignment')
    enhancements: Mapped[list['Enhancement']] = relationship('Enhancement')

    __mapper_args__ = {
        'polymorphic_identity': 'item',
        'polymorphic_on': 'type',
    }
