from typing import TypeVar

from .generic import GenericItemModel
from .twitter import TwitterItemModel
from .academic import AcademicItemModel

AnyItemModel = GenericItemModel | TwitterItemModel | AcademicItemModel
AnyItemModelList = list[TwitterItemModel] | list[AcademicItemModel] | list[GenericItemModel]
AnyItemModelType = TypeVar('AnyItemModelType', GenericItemModel, TwitterItemModel, AcademicItemModel)

__all__ = ['GenericItemModel', 'TwitterItemModel', 'AcademicItemModel',
           'AnyItemModel', 'AnyItemModelType', 'AnyItemModelList']
