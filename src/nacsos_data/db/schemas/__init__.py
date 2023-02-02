from typing import TypeVar

from ..base_class import Base
from .annotations import AnnotationScheme, Annotation, Assignment, AssignmentScope
from .bot_annotations import BotAnnotationMetaData, BotAnnotation
from .projects import Project, ProjectPermissions
from .users import User
from .imports import Import, m2m_import_item_table

from .items import ItemType, ItemTypeLiteral
from .items.base import Item
from .items.generic import GenericItem
from .items.twitter import TwitterItem
from .items.academic import AcademicItem

AnyItemType = GenericItem | TwitterItem | AcademicItem
AnyItemSchema = TypeVar('AnyItemSchema', GenericItem, TwitterItem, AcademicItem)

__all__ = ['Base',
           # Schemas for annotations
           'Annotation', 'AnnotationScheme', 'Assignment', 'AssignmentScope',
           # Schemas for "automated" annotations
           'BotAnnotationMetaData', 'BotAnnotation',
           # Schemas for items (i.e. documents) and util types
           'Item', 'GenericItem', 'TwitterItem', 'AcademicItem',
           'ItemType', 'ItemTypeLiteral', 'AnyItemSchema', 'AnyItemType',
           # Schemas for organising data
           'Import', 'm2m_import_item_table',
           # Schemas for project management
           'Project', 'ProjectPermissions', 'User']
