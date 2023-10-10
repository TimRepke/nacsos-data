from typing import TypeVar, Union

from .generic import GenericItemModel
from .twitter import TwitterItemModel
from .academic import AcademicItemModel, AcademicItemVariantModel
from .lexis_nexis import LexisNexisItemModel, LexisNexisItemSourceModel

AnyItemModel = Union[TwitterItemModel, AcademicItemModel, LexisNexisItemModel, GenericItemModel]
AnyItemModelList = list[TwitterItemModel] | list[AcademicItemModel] | list[LexisNexisItemModel] | list[GenericItemModel]
AnyItemModelType = TypeVar('AnyItemModelType',
                           GenericItemModel, TwitterItemModel,
                           AcademicItemModel, LexisNexisItemModel)

__all__ = ['GenericItemModel', 'TwitterItemModel',
           'AcademicItemModel', 'AcademicItemVariantModel',
           'LexisNexisItemModel', 'LexisNexisItemSourceModel',
           'AnyItemModel', 'AnyItemModelType', 'AnyItemModelList']
