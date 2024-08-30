from typing import TypeVar, Union, NamedTuple

from .generic import GenericItemModel
from .twitter import TwitterItemModel
from .academic import AcademicItemModel, AcademicItemVariantModel
from .lexis_nexis import LexisNexisItemModel, LexisNexisItemSourceModel, FullLexisNexisItemModel

class ItemEntry(NamedTuple):
    item_id: str
    text: str

AnyItemModel = Union[
    TwitterItemModel,
    AcademicItemModel,
    LexisNexisItemModel,
    FullLexisNexisItemModel,
    GenericItemModel
]
AnyItemModelList = (
        list[TwitterItemModel]
        | list[AcademicItemModel]
        | list[LexisNexisItemModel]
        | list[FullLexisNexisItemModel]
        | list[GenericItemModel]
)
AnyItemModelType = TypeVar('AnyItemModelType',
                           GenericItemModel,
                           TwitterItemModel,
                           AcademicItemModel,
                           LexisNexisItemModel,
                           FullLexisNexisItemModel)

__all__ = ['GenericItemModel', 'TwitterItemModel',
           'AcademicItemModel', 'AcademicItemVariantModel',
           'LexisNexisItemModel', 'LexisNexisItemSourceModel', 'FullLexisNexisItemModel',
           'AnyItemModel', 'AnyItemModelType', 'AnyItemModelList', 'ItemEntry']
