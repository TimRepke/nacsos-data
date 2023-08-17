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
    'openalex-work',
    # patent-related line encodings
    'db-patent-item'
]


class _JSONLImport(TaskParams, ABC):
    encoding: ClassVar[LineEncoding]


class TwitterDBFileImport(_JSONLImport):
    func_name: Literal['nacsos_lib.twitter.import.import_twitter_db']  # type: ignore[misc]
    encoding: Literal['db-twitter-item'] = 'db-twitter-item'  # type: ignore[misc]

    project_id: str | uuid.UUID | None = None
    import_id: str | uuid.UUID | None = None
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
    encoding: Literal['twitter-api-page'] = 'twitter-api-page'  # type: ignore[misc]

    project_id: str | uuid.UUID | None = None
    import_id: str | uuid.UUID | None = None
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


class AcademicItemImport(_JSONLImport):
    func_name: Literal['nacsos_lib.academic.import.import_academic_db']  # type: ignore[misc]
    encoding: Literal['db-academic-item'] = 'db-academic-item'  # type: ignore[misc]

    project_id: str | uuid.UUID | None = None
    import_id: str | uuid.UUID | None = None
    filenames: list[str]

    @property
    def payload(self) -> dict[str, Any]:
        return {
            'project_id': str(self.project_id),
            'import_id': str(self.import_id),
            'articles': {
                'user_serializer': 'JSONLSerializer',
                'user_dtype': 'AcademicItemModel',
                'filenames': self.filenames
            }
        }


class OpenAlexItemImport(_JSONLImport):
    func_name: Literal['nacsos_lib.academic.import.import_openalex_file']  # type: ignore[misc]
    encoding: Literal['openalex-work'] = 'openalex-work'  # type: ignore[misc]

    project_id: str | uuid.UUID | None = None
    import_id: str | uuid.UUID | None = None
    filenames: list[str]

    @property
    def payload(self) -> dict[str, Any]:
        return {
            'project_id': str(self.project_id),
            'import_id': str(self.import_id),
            'articles': {
                'user_serializer': 'JSONLSerializer',
                'user_dtype': 'WorkSolr',
                'filenames': self.filenames
            }
        }
