from enum import Enum


class ItemType(str, Enum):
    generic = 'generic'
    twitter = 'twitter'
    academic = 'academic'
    patents = 'patents'
