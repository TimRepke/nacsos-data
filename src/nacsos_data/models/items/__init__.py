from uuid import UUID

from .. import SBaseModel
from .twitter import TwitterItemModel, TwitterMetaObject, ReferencedTweet


class ItemModel(SBaseModel):
    """
    Corresponds to db.models.items.Item

    """
    # Unique identifier for this Item.
    item_id: str | UUID | None = None

    # The text for this item
    text: str

    # any kind of meta-data
    #   For project marked as "basic" this information may be shown to the user.
    #   Keys with prefix `_` will not be rendered by the frontend though.
    meta: dict


AnyItemModel = ItemModel | TwitterItemModel
