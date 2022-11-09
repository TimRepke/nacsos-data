from typing import TypeVar

from .generic import GenericItemModel
from .twitter import TwitterItemModel
from .academic import AcademicItemModel

AnyItemModel = GenericItemModel | TwitterItemModel | AcademicItemModel
AnyItemModelType = TypeVar('AnyItemModelType', GenericItemModel, TwitterItemModel, AcademicItemModel)

__all__ = ['GenericItemModel', 'TwitterItemModel', 'AnyItemModel', 'AnyItemModelType']
