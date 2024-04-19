import uuid
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Callable

from nacsos_data.db.connection import get_engine_async
from nacsos_data.models.items.academic import AcademicItemModel
from nacsos_data.models.openalex.solr import OpType, SearchField, DefType, WorkSolr
from nacsos_data.util.academic.importer import import_academic_items
from nacsos_data.util.academic.scopus import read_scopus_csv_file
from nacsos_data.util.academic.wos import read_wos_file
from nacsos_data.util.academic.openalex import generate_items_from_openalex, translate_doc, translate_work

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401


async def _import(from_sources: Callable[[], Generator[AcademicItemModel, None, None]],
                  db_config: Path,
                  logger: logging.Logger,
                  project_id: str | None = None,
                  import_id: str | None = None) -> None:
    if import_id is None:
        raise ValueError('Import ID is not set!')
    if project_id is None:
        raise ValueError('Project ID is not set!')
    db_engine = get_engine_async(conf_file=str(db_config))

    async with db_engine.session() as session:
        await import_academic_items(
            session=session,
            project_id=project_id,
            new_items=from_sources,
            import_name=None,
            description=None,
            user_id=None,
            import_id=import_id,
            vectoriser=None,
            max_slop=0.05,
            batch_size=5000,
            dry_run=False,
            trust_new_authors=False,
            trust_new_keywords=False,
            log=logger
        )


async def import_wos_files(sources: list[Path],
                           db_config: Path,
                           project_id: str | None = None,
                           import_id: str | None = None,
                           logger: logging.Logger | None = None) -> None:
    """
    Import Web of Science files in ISI format.
    Each record will be checked for duplicates within the project.

    `project_id` and `import_id` can be set to automatically populate the many-to-many tables
    and link the data to an import or project.

    **sources**
        WoS isi filenames (absolute paths)
    **project_id**
        The project_id to connect these items to (required)
    **import_id**
        The import_id to connect these items to (required)
    """

    if len(sources) == 0:
        raise ValueError('Missing source files!')

    logger = logging.getLogger('import_wos_file') if logger is None else logger

    def from_sources() -> Generator[AcademicItemModel, None, None]:
        for source in sources:
            for itm in read_wos_file(filepath=str(source), project_id=project_id):
                itm.item_id = uuid.uuid4()
                yield itm

    logger.info(f'Importing articles from web of science files: {sources}')
    await _import(from_sources, db_config=db_config, project_id=project_id, import_id=import_id, logger=logger)


async def import_scopus_csv_file(sources: list[Path],
                                 db_config: Path,
                                 project_id: str | None = None,
                                 import_id: str | None = None,
                                 logger: logging.Logger | None = None) -> None:
    """
    Import Scopus files in CSV format.
    Consult the [documentation](https://apsis.mcc-berlin.net/nacsos-docs/user/import/) before continuing!
    Each record will be checked for duplicates within the project.

    `project_id` and `import_id` can be set to automatically populate the many-to-many tables
    and link the data to an import or project.

    **records**
        An Artefact with scopus csv filenames.
    **project_id**
        The project_id to connect these items to (required)
    **import_id**
        The import_id to connect these items to (required)
    """

    if len(sources) == 0:
        raise ValueError('Missing source files!')

    logger = logging.getLogger('import_scopus_csv') if logger is None else logger

    def from_sources() -> Generator[AcademicItemModel, None, None]:
        for source in sources:
            for itm in read_scopus_csv_file(filepath=str(source), project_id=project_id):
                itm.item_id = uuid.uuid4()
                yield itm

    logger.info(f'Importing articles from scopus CSV files: {sources}')
    await _import(from_sources, db_config=db_config, project_id=project_id, import_id=import_id, logger=logger)


async def import_academic_db(sources: list[Path],
                             db_config: Path,
                             project_id: str | None = None,
                             import_id: str | None = None,
                             logger: logging.Logger | None = None) -> None:
    """
    Import articles that are in the exact format of how AcademicItems are stored in the database.
    We assume one JSON-encoded AcademicItemModel per line.

    `project_id` and `import_id` can be set to automatically populate the M2M tables
    and link the data to an import or project.

    **sources**
        An Artefact of a AcademicItems
    **project_id**
        The project_id to connect these tweets to
    **import_id**
        The import_id to connect these tweets to
    """

    if len(sources) == 0:
        raise ValueError('Missing source files!')

    logger = logging.getLogger('import_academic_file') if logger is None else logger

    def from_sources() -> Generator[AcademicItemModel, None, None]:
        for source in sources:
            with open(source, 'r') as f:
                for line in f:
                    itm = AcademicItemModel.model_validate_json(line)
                    itm.item_id = uuid.uuid4()
                    yield itm

    logger.info(f'Importing articles (AcademicItemModel-formatted) from file: {sources}')
    await _import(from_sources, db_config=db_config, project_id=project_id, import_id=import_id, logger=logger)


async def import_openalex(query: str,
                          openalex_url: str,
                          db_config: Path,
                          def_type: DefType = 'lucene',
                          field: SearchField = 'title_abstract',
                          op: OpType = 'AND',
                          project_id: str | None = None,
                          import_id: str | None = None,
                          logger: logging.Logger | None = None) -> None:
    """
    Import items from our self-hosted Solr database.
    Each record will be checked for duplicates within the project.

    `project_id` and `import_id` can be set to automatically populate the many-to-many tables
    and link the data to an import or project.

    **query**
        The solr query to run
    **def_type**
        The solr parser to use, typically this is 'lucene'
    **field**
        The field the search is executed on, often this is set to title & abstract
    **op**
        Usually you want this to be set to 'AND'
    **project_id**
        The project_id to connect these items to (required)
    **import_id**
        The import_id to connect these items to (required)
    """

    logger = logging.getLogger('import_openalex') if logger is None else logger

    def from_source() -> Generator[AcademicItemModel, None, None]:
        for itm in generate_items_from_openalex(
                query=query,
                openalex_endpoint=openalex_url,
                def_type=def_type,
                field=field,
                op=op,
                batch_size=1000,
                log=logger
        ):
            itm.item_id = uuid.uuid4()
            yield itm

    logger.info('Importing articles from OpenAlex-solr')
    await _import(from_source, db_config=db_config, project_id=project_id, import_id=import_id, logger=logger)


async def import_openalex_files(sources: list[Path],
                                db_config: Path,
                                project_id: str | None = None,
                                import_id: str | None = None,
                                logger: logging.Logger | None = None) -> None:
    """
    Import articles that are in the OpenAlex format used in our solr database.
    We assume one JSON-encoded WorkSolr object per line.

    `project_id` and `import_id` can be set to automatically populate the M2M tables
    and link the data to an import or project.

    **articles**
        An Artefact of a solr export
    **project_id**
        The project_id to connect these tweets to
    **import_id**
        The import_id to connect these tweets to
    """

    logger = logging.getLogger('import_openalex_files') if logger is None else logger

    def from_sources() -> Generator[AcademicItemModel, None, None]:
        for source in sources:
            with open(source, 'r') as f:
                for line in f:
                    itm = translate_work(translate_doc(WorkSolr.model_validate_json(line)))
                    itm.item_id = uuid.uuid4()
                    yield itm

    logger.info(f'Importing articles (WorkSolr-formatted) from files: {sources}')
    await _import(from_sources, db_config=db_config, project_id=project_id, import_id=import_id, logger=logger)
