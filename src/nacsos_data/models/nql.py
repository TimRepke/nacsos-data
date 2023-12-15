from typing import ForwardRef, TypeAlias

from pydantic import BaseModel, TypeAdapter
from typing import Literal

Comparator = str  # Literal['=', '>=', '<=', '<', '>', '!=']
ComparatorExt = str  # Literal['=', '>=', '<=', '<', '>', '!=', 'LIKE', 'SIMILAR']
SetComparator = str  # Literal['==', '@>', '!>', '&&']

FieldA = Literal['title', 'abstract', 'pub_year', 'date', 'source']
FieldB = Literal['doi', 'item_id', 'openalex_id']
Field = FieldA | FieldB | Literal['meta']


class IEUUID(BaseModel):
    incl: bool
    uuid: str


class FieldFilter(BaseModel):
    field: FieldA
    value: str | int
    comp: ComparatorExt


class FieldFilters(BaseModel):
    field: FieldB
    values: list[str]


class MetaFilter(BaseModel):
    filter: Literal['meta'] = 'meta'
    field: str
    comp: ComparatorExt
    value: str | int | bool


class ImportFilter(BaseModel):
    filter: Literal['import'] = 'import'
    import_ids: list[IEUUID]


class UsersFilter(BaseModel):
    user_ids: list[str]
    mode: Literal['ALL', 'ANY']


class _LabelFilter(BaseModel):
    filter: Literal['label'] = 'label'
    scopes: list[str] | None = None
    users: UsersFilter | None = None
    repeats: list[int] | None = None
    key: str
    type: Literal['user', 'bot', 'resolved']


class LabelFilterInt(_LabelFilter):
    value_int: int | None = None
    comp: Comparator


class LabelFilterBool(_LabelFilter):
    comp: Literal['='] = '='
    value_bool: bool | None = None


class LabelFilterMulti(_LabelFilter):
    multi_int: list[int] | None = None
    comp: SetComparator


class AssignmentFilter(BaseModel):
    filter: Literal['assignment']
    mode: int
    scopes: list[str] | None = None


class AnnotationFilter(BaseModel):
    filter: Literal['annotation']
    incl: bool
    scopes: list[str] | None


NQLFilter: TypeAlias = ForwardRef('NQLFilter')  # type: ignore[valid-type]


class SubQuery(BaseModel):
    and_: list[NQLFilter] | None = None
    or_: list[NQLFilter] | None = None


NQLFilter = (FieldFilter
             | FieldFilters
             | LabelFilterMulti
             | LabelFilterBool
             | LabelFilterInt
             | AssignmentFilter
             | AnnotationFilter
             | ImportFilter
             | MetaFilter
             | SubQuery)

SubQuery.model_rebuild()

NQLFilterParser = TypeAdapter(NQLFilter)
