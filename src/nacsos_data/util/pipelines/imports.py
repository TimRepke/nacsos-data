import logging
from typing import Any, ClassVar, Type, Literal
from abc import ABC, abstractmethod
from uuid import UUID

import httpx
from httpx import HTTPError
from sqlalchemy import select

from ...db import DatabaseEngineAsync
from ...db.schemas.imports import Import
from ...models.imports import \
    ImportConfigJSONL, \
    LineEncoding, \
    ImportModel, \
    ImportConfigWoS, \
    Type2Conf, \
    ImportTypeLiteral, ImportConfigScopus

logger = logging.getLogger('nacsos_data.util.pipelines')

FileType = Literal['wos-file', 'scopus-file']


class Converter(ABC):
    func_name: ClassVar[str]

    @staticmethod
    @abstractmethod
    def convert_details(import_details: ImportModel) -> dict[str, Any]:
        ...


class GenericConverter(Converter):
    @property
    @abstractmethod
    def encoding(self) -> FileType:
        ...


class JSONLConverter(Converter):
    @property
    @abstractmethod
    def encoding(self) -> LineEncoding:
        ...


class TwitterConverter(JSONLConverter):
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


class TwitterApiConverter(JSONLConverter):
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

class WebOfScienceConverter(Converter):
    encoding: FileType = 'wos-file'
    func_name = 'nacsos_lib.academic.import.import_wos_file'

    @staticmethod
    def convert_details(import_details: ImportModel) -> dict[str, Any]:
        if type(import_details.config) != ImportConfigWoS:
            raise AttributeError('Incompatible import details config.')
        return {
            'project_id': str(import_details.project_id),
            'import_id': str(import_details.import_id),
            'records': {
                'user_serializer': 'WebOfScienceSerializer',
                'user_dtype': 'AcademicItemModel',
                'filenames': import_details.config.filenames
            }
        }


class ScopusConverter(Converter):
    encoding: FileType = 'scopus-file'
    func_name = 'nacsos_lib.academic.import.import_scopus_file'

    @staticmethod
    def convert_details(import_details: ImportModel) -> dict[str, Any]:
        if type(import_details.config) != ImportConfigScopus:
            raise AttributeError('Incompatible import details config.')
        return {
            'project_id': str(import_details.project_id),
            'import_id': str(import_details.import_id),
            'records': {
                'user_serializer': 'ScopusSerializer',
                'user_dtype': 'AcademicItemModel',
                'filenames': import_details.config.filenames
            }
        }


def get_jsonl_converter(line_type: LineEncoding) -> Type[JSONLConverter] | None:
    for sc in JSONLConverter.__subclasses__():
        if sc.encoding == line_type:  # type: ignore[comparison-overlap]
            return sc
    return None


def get_generic_converter(line_type: FileType) -> Type[GenericConverter] | None:
    for sc in GenericConverter.__subclasses__():
        if sc.encoding == line_type:  # type: ignore[comparison-overlap]
            return sc  # type: ignore[unreachable] # FIXME
    return None


class UndefinedEncoding(Exception):
    pass


class FailedJobSubmission(Exception):
    pass


class ImportDetailsNotFound(Exception):
    pass


async def _submit_import_task(import_id: UUID | str,
                              base_url: str,
                              auth_token: str,
                              engine: DatabaseEngineAsync) -> str:
    async with httpx.AsyncClient() as client, engine.session() as session:
        stmt = select(Import).where(Import.import_id == import_id)
        import_details: Import | None = (await session.scalars(stmt)).one_or_none()
        if import_details is None:
            raise ImportDetailsNotFound(f"No import found in db for id {import_id}")

        converter: Type[JSONLConverter] | Type[WebOfScienceConverter] | None = None

        logger.debug(f'Import config type: {import_details.type} ({type(import_details.config)})')

        conf_type: ImportTypeLiteral = import_details.type.value
        ConfigModel = Type2Conf.get(conf_type)

        if ConfigModel is None:
            raise UndefinedEncoding(f'This import type ({import_details.type}) has no known pipeline function.')

        config = ConfigModel.model_validate(import_details.config)
        if type(config) == ImportConfigJSONL:
            converter = get_jsonl_converter(config.line_type)
        elif type(config) == ImportConfigWoS:
            converter = WebOfScienceConverter

        if config is None or converter is None:
            raise UndefinedEncoding('Could not find a proper converter for this config.')

        payload = {
            'task_id': None,
            'function_name': converter.func_name,
            'params': converter.convert_details(ImportModel.model_validate(import_details.__dict__)),
            'user_id': str(import_details.user_id),
            'project_id': str(import_details.project_id),
            'comment': f'Import for "{import_details.name}" ({import_id})',
            'location': 'LOCAL',
            'force_run': True,
            'forced_dependencies': None,
        }

        try:
            response = await client.put(f'{base_url}/queue/submit/task',
                                        json=payload,
                                        headers={
                                            'Authorization': f'Bearer {auth_token}',
                                            'X-Project-ID': str(import_details.project_id)
                                        })
            if response.status_code != 200:
                error = response.json()
                raise FailedJobSubmission('Failed to submit job', payload, error)

            task_id: str = response.json()

            # remember that we submitted this import job (and its reference)
            import_details.pipeline_task_id = task_id
            await session.commit()

            return task_id
        except HTTPError as e:
            logger.exception(e)
            raise FailedJobSubmission('Failed to submit job', repr(e))


async def submit_wos_import_task(import_id: UUID | str,
                                 base_url: str, auth_token: str,
                                 engine: DatabaseEngineAsync) -> str:
    return await _submit_import_task(import_id=import_id,
                                     base_url=base_url,
                                     auth_token=auth_token,
                                     engine=engine)


async def submit_jsonl_import_task(import_id: UUID | str,
                                   base_url: str, auth_token: str,
                                   engine: DatabaseEngineAsync) -> str:
    return await _submit_import_task(import_id=import_id,
                                     base_url=base_url,
                                     auth_token=auth_token,
                                     engine=engine)
