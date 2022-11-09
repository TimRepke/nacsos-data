from typing import TypeVar

from ..base_class import Base
from .annotations import AnnotationScheme, Annotation, Assignment, AssignmentScope
from .bot_annotations import BotAnnotationMetaData, BotAnnotation
from .projects import Project, ProjectPermissions
from .users import User
from .imports import Import, M2MImportItem

from .items import ItemType
from .items.base import Item
from .items.generic import GenericItem
from .items.twitter import TwitterItem

# from .items.academic import AcademicItem # FIXME uncomment

AnyItemType = Item | TwitterItem
AnyItemSchema = TypeVar('AnyItemSchema', Item, TwitterItem)

__all__ = ['Base',
           # Schemas for annotations
           'Annotation', 'AnnotationScheme', 'Assignment', 'AssignmentScope',
           # Schemas for "automated" annotations
           'BotAnnotationMetaData', 'BotAnnotation',
           # Schemas for items (i.e. documents) and util types
           'Item', 'GenericItem', 'TwitterItem',  # 'AcademicItem',
           'AnyItemSchema', 'AnyItemType',
           # Schemas for organising data
           'Import', 'M2MImportItem',
           # Schemas for project management
           'Project', 'ProjectPermissions', 'User']
