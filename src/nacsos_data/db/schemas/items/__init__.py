from enum import Enum
from typing import Literal


class ItemType(str, Enum):
    generic = 'generic'
    twitter = 'twitter'
    academic = 'academic'
    patents = 'patents'
    lexis = 'lexis'


ItemTypeLiteral = Literal['generic', 'twitter', 'academic', 'patents', 'lexis']
