from uuid import UUID

from .. import SBaseModel


class ItemModel(SBaseModel):
    """
    Corresponds to db.models.items.Item

    """
    # Unique identifier for this Item.
    item_id: str | UUID | None = None

    # The text for this item
    text: str
