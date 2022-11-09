from uuid import UUID
from pydantic import BaseModel


class ItemModel(BaseModel):
    """
    Corresponds to db.models.items.Item

    """
    # Unique identifier for this Item.
    item_id: str | UUID | None = None

    # The text for this item
    text: str
