from pydantic import BaseModel
from typing import Optional


class ItemModel(BaseModel):
    """
    Corresponds to db.models.items.Item

    User represents a person.
    Most entries in the database will be (indirectly) linked to user accounts, so this is
    at the core of access management and ownership.
    """
    # Unique identifier for this Item.
    item_id: Optional[str]

    # The text for this item
    text: str
