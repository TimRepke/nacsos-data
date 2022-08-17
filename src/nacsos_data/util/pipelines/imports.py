import logging
from typing import Callable, Any
from uuid import UUID

import httpx

from ...db import DatabaseEngineAsync
from ...db.crud.imports import read_import, upsert_import
from ...models.imports import ImportConfigJSONL, LineEncoding, ImportModel


logger = logging.getLogger('nacsos_data.util.pipelines')

function_map: dict[LineEncoding, (str, Callable[[ImportModel], dict[str, Any]])] = {
    'db-twitter-item': ('nacsos_lib.twitter.import.import_twitter_db',
                        lambda import_details: {
                            'project_id': str(import_details.project_id),
                            'import_id': str(import_details.import_id),
                            'tweets': {
                                'user_serializer': 'JSONLSerializer',
                                'user_dtype': 'TwitterItemModel',
                                'filenames': import_details.config.filenames
                            }
                        }),
    'twitter-api-page': ('nacsos_lib.twitter.import.import_twitter_api',
                         lambda import_details: {
                             'project_id': str(import_details.project_id),
                             'import_id': str(import_details.import_id),
                             'tweet_api_pages': {
                                 'user_serializer': 'JSONLSerializer',
                                 'user_dtype': 'TwitterItemModel',
                                 'filename': import_details.config.filenames[0]
                             }
                         }),
    # 'db-basic-item': '',
    # 'db-academic-item': '',
    # 'db-patent-item': ''
}


class UndefinedJSONLEncoding(Exception):
    pass


class FailedJobSubmission(Exception):
    pass


async def submit_jsonl_import_task(import_id: UUID | str,
                                   base_url: str,
                                   engine: DatabaseEngineAsync) -> str:
    import_details = await read_import(import_id=import_id, engine=engine)
    async with httpx.AsyncClient() as client:
        config: ImportConfigJSONL = import_details.config

        if config.line_type not in function_map:
            raise UndefinedJSONLEncoding(f'Line encoding "{config.line_type}" has no matching pipeline task (yet).')

        payload = {
            'task_id': None,
            'function_name': function_map[config.line_type][0],
            'params': function_map[config.line_type][1](import_details),
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

        task_id = response.text

    # remember that we submitted this import job (and its reference)
    import_details.pipeline_task_id = task_id
    await upsert_import(import_model=import_details, engine=engine)

    return task_id
