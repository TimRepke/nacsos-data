from typing import Literal
from .. import SBaseModel

LineEncoding = Literal[
    # twitter-related line encodings
    'db-twitter-item', 'twitter-api-page',
    # basic (generic) items per line
    'db-basic-item',
    # academic-related line encodings
    'db-academic-item',
    # patent-related line encodings
    'db-patent-item'
]


class ImportConfigJSONL(SBaseModel):
    filenames: list[str]
    line_type: LineEncoding
