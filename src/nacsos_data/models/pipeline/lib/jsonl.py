import uuid
from abc import ABC
from typing import Any, ClassVar, Literal
from .abc import TaskParams

LineEncoding = Literal[
    # twitter-related line encodings
    'db-twitter-item',
    'twitter-api-page',
    # basic (generic) items per line
    'db-generic-item',
    # academic-related line encodings
    'db-academic-item',
    # patent-related line encodings
    'db-patent-item'
]


class _JSONLImport(TaskParams, ABC):
    encoding: ClassVar[LineEncoding]


class TwitterDBFileImport(_JSONLImport):
    func_name: Literal['nacsos_lib.twitter.import.import_twitter_db']  # type: ignore[misc]
    encoding: str = 'db-twitter-item'

    project_id: str | uuid.UUID
    import_id: str | uuid.UUID
    filenames: list[str]

    @property
    def payload(self) -> dict[str, Any]:
        return {
            'project_id': str(self.project_id),
            'import_id': str(self.import_id),
            'tweets': {
                'user_serializer': 'JSONLSerializer',
                'user_dtype': 'TwitterItemModel',
                'filenames': self.filenames
            }
        }


class TwitterAPIFileImport(_JSONLImport):
    func_name: Literal['nacsos_lib.twitter.import.import_twitter_api']  # type: ignore[misc]
    encoding: str = 'twitter-api-page'

    project_id: str | uuid.UUID
    import_id: str | uuid.UUID
    filenames: list[str]

    @property
    def payload(self) -> dict[str, Any]:
        return {
            'project_id': str(self.project_id),
            'import_id': str(self.import_id),
            'tweet_api_pages': {
                'user_serializer': 'JSONLSerializer',
                'user_dtype': 'TwitterItemModel',
                'filenames': self.filenames[0]
            }
        }
