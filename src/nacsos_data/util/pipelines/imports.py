import logging

import httpx
from httpx import HTTPError
from sqlalchemy import select

from ...db import DatabaseEngineAsync
from ...db.schemas.imports import Import
from ...models.imports import ImportModel

logger = logging.getLogger('nacsos_data.util.pipelines')


class UndefinedEncoding(Exception):
    pass


class FailedJobSubmission(Exception):
    pass


class ImportDetailsNotFound(Exception):
    pass


async def submit_import_task(import_details: ImportModel,
                             base_url: str,
                             auth_token: str,
                             engine: DatabaseEngineAsync) -> str:
    async with httpx.AsyncClient() as client, engine.session() as session:
        stmt = select(Import).where(Import.import_id == import_details.import_id)
        import_orm: Import | None = (await session.scalars(stmt)).one_or_none()
        if import_orm is None:
            raise ImportDetailsNotFound(f'No import found in db for id {import_details.import_id}')

        logger.debug(f'Import config type: {import_details.type} ({type(import_details.config)})')
        config = import_details.config
        if config is None:
            raise UndefinedEncoding('The import config is empty!')

        try:
            response = await client.put(f'{base_url}/queue/submit/task',
                                        json=config.payload,
                                        headers={
                                            'Authorization': f'Bearer {auth_token}',
                                            'X-Project-ID': str(import_details.project_id)
                                        })
            if response.status_code != 200:
                error = response.json()
                raise FailedJobSubmission('Failed to submit job', config.payload, error)

            task_id: str = response.json()

            # remember that we submitted this import job (and its reference)
            import_orm.pipeline_task_id = task_id
            await session.commit()

            return task_id
        except HTTPError as e:
            logger.exception(e)
            raise FailedJobSubmission('Failed to submit job', repr(e))
