from typing import ForwardRef, TypeAlias

from pydantic import BaseModel, TypeAdapter, Field as PField
from typing import Literal

from typing_extensions import Annotated

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
    filter: Literal['field'] = 'field'
    field: FieldA
    value: str | int
    comp: ComparatorExt | None = None


class FieldFilters(BaseModel):
    filter: Literal['field_mul'] = 'field_mul'
    field: FieldB
    values: list[str]


class _MetaFilter(BaseModel):
    # filter: Literal['meta'] = 'meta'
    field: str


class MetaFilterBool(_MetaFilter):
    filter: Literal['meta_bool'] = 'meta_bool'
    value_type: Literal['bool'] = 'bool'
    comp: Literal['='] = '='
    value: bool


class MetaFilterInt(_MetaFilter):
    filter: Literal['meta_int'] = 'meta_int'
    value_type: Literal['int'] = 'int'
    comp: Comparator
    value: int


class MetaFilterStr(_MetaFilter):
    filter: Literal['meta_str'] = 'meta_str'
    value_type: Literal['str'] = 'str'
    comp: Literal['LIKE'] = 'LIKE'
    value: str


MetaFilter = Annotated[MetaFilterBool
                       | MetaFilterInt
                       | MetaFilterStr, PField(discriminator='value_type')]


class ImportFilter(BaseModel):
    filter: Literal['import'] = 'import'
    import_ids: list[IEUUID]


class UsersFilter(BaseModel):
    user_ids: list[str]
    mode: Literal['ALL', 'ANY']


class _LabelFilter(BaseModel):
    # filter: Literal['label'] = 'label'
    scopes: list[str] | None = None
    scheme: str | None = None
    users: UsersFilter | None = None
    repeats: list[int] | None = None
    key: str
    type: Literal['user', 'bot', 'resolved']


class LabelFilterInt(_LabelFilter):
    filter: Literal['label_int'] = 'label_int'
    value_type: Literal['int'] = 'int'
    value_int: int | None = None
    comp: Comparator


class LabelFilterBool(_LabelFilter):
    filter: Literal['label_bool'] = 'label_bool'
    value_type: Literal['bool'] = 'bool'
    comp: Literal['='] = '='
    value_bool: bool | None = None


class LabelFilterMulti(_LabelFilter):
    filter: Literal['label_multi'] = 'label_multi'
    value_type: Literal['multi'] = 'multi'
    multi_int: list[int] | None = None
    comp: SetComparator


LabelFilter = Annotated[LabelFilterInt
                        | LabelFilterBool
                        | LabelFilterMulti, PField(discriminator='value_type')]


class AssignmentFilter(BaseModel):
    filter: Literal['assignment'] = 'assignment'
    mode: int
    scopes: list[str] | None = None
    scheme: str | None = None


class AnnotationFilter(BaseModel):
    filter: Literal['annotation'] = 'annotation'
    incl: bool
    scopes: list[str] | None = None
    scheme: str | None = None


NQLFilter: TypeAlias = ForwardRef('NQLFilter')  # type: ignore[valid-type]


class SubQuery(BaseModel):
    filter: Literal['sub'] = 'sub'
    and_: list[NQLFilter] | None = None
    or_: list[NQLFilter] | None = None


NQLFilter = Annotated[FieldFilter
                      | FieldFilters
                      | LabelFilterMulti
                      | LabelFilterBool
                      | LabelFilterInt
                      | AssignmentFilter
                      | AnnotationFilter
                      | ImportFilter
                      | MetaFilterBool
                      | MetaFilterInt
                      | MetaFilterStr
                      | SubQuery, PField(discriminator='filter')]

SubQuery.model_rebuild()

NQLFilterParser = TypeAdapter(NQLFilter)
