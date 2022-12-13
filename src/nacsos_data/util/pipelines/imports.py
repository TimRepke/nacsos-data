import logging
from typing import Any, ClassVar, Type
from abc import ABC, abstractmethod
from uuid import UUID

import httpx

from ...db import DatabaseEngineAsync
from ...db.crud.imports import read_import
from ...models.imports import ImportConfigJSONL, ImportConfigWoS, LineEncoding, ImportModel

logger = logging.getLogger('nacsos_data.util.pipelines')


class Converter(ABC):
    @property
    @abstractmethod
    def encoding(self) -> LineEncoding:
        ...

    func_name: ClassVar[str]

    @staticmethod
    @abstractmethod
    def convert_details(import_details: ImportModel) -> dict[str, Any]:
        ...


class TwitterConverter(Converter):
    encoding: LineEncoding = 'db-twitter-item'
    func_name = 'nacsos_lib.twitter.import.import_twitter_db'

    @staticmethod
    def convert_details(import_details: ImportModel) -> dict[str, Any]:
        if type(import_details.config) != ImportConfigJSONL:
            raise AttributeError('Incompatible import details config.')
        return {
            'project_id': str(import_details.project_id),
            'import_id': str(import_details.import_id),
            'tweets': {
                'user_serializer': 'JSONLSerializer',
                'user_dtype': 'TwitterItemModel',
                'filenames': import_details.config.filenames
            }
        }


class TwitterApiConverter(Converter):
    encoding: LineEncoding = 'twitter-api-page'
    func_name = 'nacsos_lib.twitter.import.import_twitter_api'

    @staticmethod
    def convert_details(import_details: ImportModel) -> dict[str, Any]:
        if type(import_details.config) != ImportConfigJSONL:
            raise AttributeError('Incompatible import details config.')
        return {
            'project_id': str(import_details.project_id),
            'import_id': str(import_details.import_id),
            'tweet_api_pages': {
                'user_serializer': 'JSONLSerializer',
                'user_dtype': 'TwitterItemModel',
                'filename': import_details.config.filenames[0]
            }
        }


# 'db-basic-item': '',
# 'db-academic-item': '',
# 'db-patent-item': ''


def get_converter(line_type: LineEncoding) -> Type[Converter] | None:
    for sc in Converter.__subclasses__():
        if sc.encoding == line_type:  # type: ignore[comparison-overlap]
            return sc
    return None


class UndefinedJSONLEncoding(Exception):
    pass


class FailedJobSubmission(Exception):
    pass


class ImportDetailsNotFound(Exception):
    pass

async def submit_wos_import_task(import_id: UUID | str,
                                 base_url: str,
                                 engine: DatabaseEngineAsync) -> str:
    import_details = await read_import(import_id=import_id, engine=engine)
    if import_details is None:
        raise ImportDetailsNotFound(f"No import found in db for id {import_id}")

    async with httpx.AsyncClient() as client, engine.session() as session:

        params = {
            "project_id": str(import_details.project_id),
            "import_id": str(import_details.import_id),
            "records": import_details.config.filenames
        }

        print(import_details.config)

        payload = {
            'task_id': None,
            'function_name': 'nacsos_lib.academic.import.import_wos_file',
            'params': params,
            'user_id': str(import_details.user_id),
            'project_id': str(import_details.project_id),
            'comment': f'Import for "{import_details.name}" ({import_id})',
            'location': 'LOCAL',
            'force_run': True,
            'forced_dependencies': None,
        }
        response = await client.put(f'{base_url}/queue/submit/task', json=payload)

        if response.status_code != 200:
            error = response.json()
            raise FailedJobSubmission('Failed to submit job', payload, error)

        task_id: str = response.json()

        # remember that we submitted this import job (and its reference)
        import_details.pipeline_task_id = task_id
        await session.commit()


async def submit_jsonl_import_task(import_id: UUID | str,
                                   base_url: str,
                                   engine: DatabaseEngineAsync) -> str:
    import_details = await read_import(import_id=import_id, engine=engine)
    if import_details is None:
        raise ImportDetailsNotFound(f'No import found in db for id {import_id}')

    async with httpx.AsyncClient() as client, engine.session() as session:
        assert type(import_details.config) == ImportConfigJSONL
        config: ImportConfigJSONL = import_details.config

        converter = get_converter(config.line_type)
        if converter is None:
            raise UndefinedJSONLEncoding(f'Line encoding "{config.line_type}" has no matching pipeline task (yet).')

        payload = {
            'task_id': None,
            'function_name': converter.func_name,
            'params': converter.convert_details(import_details),
            'user_id': str(import_details.user_id),
            'project_id': str(import_details.project_id),
            'comment': f'Import for "{import_details.name}" ({import_id})',
            'location': 'LOCAL',
            'force_run': True,
            'forced_dependencies': None,
        }
        response = await client.put(f'{base_url}/queue/submit/task', json=payload)

        if response.status_code != 200:
            error = response.json()
            raise FailedJobSubmission('Failed to submit job', payload, error)

        task_id: str = response.json()

        # remember that we submitted this import job (and its reference)
        import_details.pipeline_task_id = task_id
        await session.commit()

    return task_id
