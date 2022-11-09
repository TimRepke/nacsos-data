from typing import Any
from .base import ItemModel

ItemMetaType = dict[str, Any]  # dict[str, str | float | int | 'ItemMetaType']


class GenericItemModel(ItemModel):
    """
    Corresponds to db.models.items.generic.GenericItem
    """
    # any kind of meta-data
    #   For project marked as "basic" this information may be shown to the user.
    #   Keys with prefix `_` will not be rendered by the frontend though.
    meta: ItemMetaType
