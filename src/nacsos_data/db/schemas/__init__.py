from typing import TypeVar

from ..base_class import Base
from .annotations import AnnotationScheme, Annotation, Assignment, AssignmentScope
from .projects import Project, ProjectPermissions
from .users import User
from .imports import Import
from .items import Item, M2MProjectItem, M2MImportItem
from .items.twitter import TwitterItem

AnyItemType = Item | TwitterItem
AnyItemSchema = TypeVar('AnyItemSchema', Item, TwitterItem)

__all__ = ['Base', 'Annotation', 'AnnotationScheme', 'Assignment', 'AssignmentScope',
           'Project', 'ProjectPermissions', 'User', 'Item', 'Import',
           'M2MImportItem', 'M2MProjectItem', 'TwitterItem', 'AnyItemSchema']
