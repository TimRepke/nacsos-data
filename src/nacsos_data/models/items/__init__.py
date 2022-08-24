from typing import TypeVar, Any
from uuid import UUID
from pydantic import BaseModel
from .twitter import TwitterItemModel, TwitterMetaObject, ReferencedTweet

ItemMetaType = dict[str, Any]  # dict[str, str | float | int | 'ItemMetaType']


class ItemModel(BaseModel):
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
    meta: ItemMetaType


AnyItemModel = ItemModel | TwitterItemModel
AnyItemModelType = TypeVar('AnyItemModelType', ItemModel, TwitterItemModel)

__all__ = ['ItemModel', 'TwitterItemModel', 'AnyItemModel', 'ItemMetaType', 'AnyItemModelType']
