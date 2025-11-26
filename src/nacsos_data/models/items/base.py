from datetime import datetime
from uuid import UUID
from pydantic import BaseModel

from nacsos_data.db.schemas import ItemType


class ItemModel(BaseModel):
    """
    Corresponds to db.models.items.Item

    """

    # Unique identifier for this Item.
    item_id: str | UUID | None = None
    # 1:N relationship to project
    project_id: str | UUID | None = None
    # type of this item
    type: ItemType | None = None
    # Date when this item was edited; if None, considered unedited; if not None, consider to never auto-update
    time_edited: datetime | None = None

    # The text for this item
    text: str | None = None
