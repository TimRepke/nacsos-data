from sqlalchemy import String, Column
from sqlalchemy.dialects.postgresql import UUID
import uuid

from ...base_class import Base


class Item(Base):
    """
    User represents a person.
    Most entries in the database will be (indirectly) linked to user accounts, so this is
    at the core of access management and ownership.
    """
    __tablename__ = 'item'

    # Unique identifier for this Item.
    item_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                     nullable=False, unique=True, index=True)

    # The text for this item
    #   Tweet: status_text
    #   Paper: abstract
    text = Column(String, nullable=False)

    # FIXME: fundamental question is how to deal with different use cases.
    #        e.g. for papers, text could be the abstract, title,  full-text, paragraphs of full text
    #             and based on context, the same item (?) would point to different texts
    #             alternatively, we view the specific item as the unique reference and Item as the context-sensitive one
    #             which would lead to lots of repeated data though
