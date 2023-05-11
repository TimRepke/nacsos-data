from typing import Any

from ...models.items import AcademicItemModel


def clear_empty(obj: Any | None) -> Any | None:
    if obj is None:
        return None

    if isinstance(obj, str):
        if len(obj) == 0:
            return None
        return obj

    if isinstance(obj, list):
        tmp_l = [clear_empty(li) for li in obj]
        tmp_l = [li for li in tmp_l if li is not None]
        if len(tmp_l) > 0:
            return tmp_l
        return None

    if isinstance(obj, dict):
        tmp_d = {key: clear_empty(val) for key, val in obj.items()}
        tmp_d = {key: val for key, val in tmp_d.items() if val is not None}
        if len(tmp_d) > 0:
            return tmp_d
        return None

    return obj


def get_cleaned_meta_field(item: AcademicItemModel) -> dict[str, Any] | None:
    """
    This will remove empty values (and respective keys) from the meta-data object of an AcademicItem.
    :param item:
    :return:
    """

    return clear_empty(item.meta)
