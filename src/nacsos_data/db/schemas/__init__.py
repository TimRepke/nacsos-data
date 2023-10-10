from typing import TypeVar

from ..base_class import Base
from .annotations import AnnotationScheme, Annotation, Assignment, AssignmentScope
from .bot_annotations import BotAnnotationMetaData, BotAnnotation
from .projects import Project, ProjectPermissions
from .users import User, AuthToken
from .imports import Import, m2m_import_item_table
from .pipeline import Task
from .highlight import Highlighter
from .annotation_tracker import AnnotationTracker

from .items import ItemType, ItemTypeLiteral
from .items.base import Item
from .items.generic import GenericItem
from .items.twitter import TwitterItem
from .items.academic import AcademicItem, AcademicItemVariant
from .items.lexis_nexis import LexisNexisItem, LexisNexisItemSource

AnyItemType = GenericItem | TwitterItem | AcademicItem | LexisNexisItem
AnyItemSchema = TypeVar('AnyItemSchema', GenericItem, TwitterItem, AcademicItem, LexisNexisItem)

__all__ = ['Base',
           'User', 'AuthToken',
           # Schemas for annotations
           'Annotation', 'AnnotationScheme', 'Assignment', 'AssignmentScope',
           # Schemas for "automated" annotations
           'BotAnnotationMetaData', 'BotAnnotation',
           # Schemas for items (i.e. documents) and util types
           'Item', 'GenericItem', 'TwitterItem',
           'AcademicItem', 'AcademicItemVariant',
           'LexisNexisItem', 'LexisNexisItemSource',
           'ItemType', 'ItemTypeLiteral', 'AnyItemSchema', 'AnyItemType',
           # Schemas for organising data
           'Import', 'm2m_import_item_table',
           # Schemas for project management
           'Project', 'ProjectPermissions', 'User',
           # Schemas for pipelines
           'Task',
           # Schemas for text highlighting
           'Highlighter',
           # Schemas for annotation statistics
           'AnnotationTracker']
