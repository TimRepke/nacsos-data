from typing import Any

from .. import clear_empty
from ...models.items import AcademicItemModel


def get_cleaned_meta_field(item: AcademicItemModel) -> dict[str, Any] | None:
    """
    This will remove empty values (and respective keys) from the meta-data object of an AcademicItem.
    :param item:
    :return:
    """

    return clear_empty(item.meta)
