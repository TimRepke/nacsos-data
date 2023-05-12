from typing import TypeVar, Union

from .generic import GenericItemModel
from .twitter import TwitterItemModel
from .academic import AcademicItemModel, AcademicItemVariantModel

AnyItemModel = Union[TwitterItemModel, AcademicItemModel, GenericItemModel]
AnyItemModelList = list[TwitterItemModel] | list[AcademicItemModel] | list[GenericItemModel]
AnyItemModelType = TypeVar('AnyItemModelType', GenericItemModel, TwitterItemModel, AcademicItemModel)

__all__ = ['GenericItemModel', 'TwitterItemModel', 'AcademicItemModel', 'AcademicItemVariantModel',
           'AnyItemModel', 'AnyItemModelType', 'AnyItemModelList']
