import uuid
import logging
import datetime
from pathlib import Path
from typing import Generator

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401
from sqlalchemy.exc import IntegrityError
from psycopg.errors import UniqueViolation, OperationalError
from sklearn.feature_extraction.text import CountVectorizer

from ..duplicate import ItemEntry, DuplicateIndex
from ...db.crud.imports import get_or_create_import
from ...db import DatabaseEngineAsync, get_engine_async
from ...db.crud.items.academic import AcademicItemGenerator, read_item_entries_from_db, gen_academic_entries, itm2txt
from ...db.schemas import AcademicItem, m2m_import_item_table, Import
from ...models.imports import M2MImportItemType
from .clean import get_cleaned_meta_field
from .duplicate import str_to_title_slug, find_duplicates, duplicate_insertion
from ...models.items import AcademicItemModel
from ...models.openalex.solr import DefType, SearchField, OpType


async def import_academic_items(
        db_engine: DatabaseEngineAsync,
        project_id: str | uuid.UUID,
        new_items: AcademicItemGenerator,
        import_name: str | None = None,
        import_id: str | uuid.UUID | None = None,
        user_id: str | uuid.UUID | None = None,
        description: str | None = None,
        vectoriser: CountVectorizer | None = None,
        max_slop: float = 0.05,
        min_text_len: int = 300,
        batch_size: int = 2000,
        dry_run: bool = True,
        trust_new_authors: bool = False,
        trust_new_keywords: bool = False,
        logger: logging.Logger | None = None
) -> tuple[str, list[str]]:
    """
    Helper function for programmatically importing `AcademicItem`s into the platform.

    Example usage:
    ```
    from nacsos_data.db import get_engine_async
    from nacsos_data.util.academic.scopus import read_scopus_file
    from nacsos_data.util.academic.import import import_academic_items

    PROJECT_ID = '??'

    db_engine = get_engine_async(conf_file='/path/to/remote_config.env')
    scopus_works = read_scopus_file('/path/to/scopus.csv', project_id=PROJECT_ID)
    await import_academic_items(items=scopus_works, db_engine=db_engine, project_id=PROJECT_ID, ...)
    ```

    If you are working in a synchronous context, you can wrap the above code in a method and run with asyncio:
    ```
    import asyncio

    def main():
        ...

    if __name__ == '__main__':
        asyncio.run(main())
    ```

    Items are always associated with a project, and within a project with an `Import`.
    This is used to indicate a "scope" of where the data comes from, e.g. a query from WoS or Scopus.
    Item sets may overlap between Imports.

    There are two modes:
      1) You can use an existing Import by providing the `import_id`.
         This might be useful when you already created a blank import via the WebUI or
         want to add more items to that import. Note, that we recommend creating a new import if
         the "scopes" are better semantically separate (e.g. query results from different points in time might be
         two Imports rather than one Import that is added to).
         In this case `user_id`, `import_name`, `description` are ignored.
      2) Create a new Import by setting `user_id`, `import_name`, and `description`; optionally set `import_id`.
         In this way, a new Import will be created and all items will be associated with that.

    :param db_engine
    :param trust_new_authors:
    :param trust_new_keywords:
    :param new_items: A list (or generator) of AcademicItems
    :param vectoriser
    :param max_slop
    :param min_text_len
    :param batch_size
    :param project_id: ID of the project the items should be added to
    :param import_id: (optional) ID to existing Import
    :param user_id: (your) user_id, which this import will be associated with
    :param import_name: Concise and descriptive name for this import
    :param description: Proper (markdown) description for this import.
                        Usually this should describe the source of the dataset and, if applicable, the search query.
    :param dry_run: If false, actually write data to the database;
                    If true, simulate best as possible (note, that duplicates within the `items` are not validated
                                                        and not all constraints can be checked)
    :param logger:
    :return: import_id and list of item_ids that were actually used in the end
    """
    if logger is None:
        logger = logging.getLogger('nacsos_data.util.academic.import')

    if project_id is None:
        raise AttributeError('You have to provide a project ID!')

    # @Max TODO
    # get sets of identifiers (e.g. using read_ids_from_db)
    # run some performance tests (esp. RAM usage at scale)
    # define wrapper, sth like
    # def filter_known_ids(dict of ids, generator):
    #   for e in generator:
    #     if not any([e[k] in ids for k, ids in dict.items()]):
    #       yield e

    item_ids: list[str] = []
    async with db_engine.session() as session:
        import_orm = await get_or_create_import(session=session,
                                                project_id=project_id,
                                                import_id=import_id,
                                                user_id=user_id,
                                                import_name=import_name,
                                                description=description,
                                                i_type='script')
        import_id = str(import_orm.import_id)

        logger.info('Creating abstract duplicate detection index')
        index = DuplicateIndex(
            existing_items=read_item_entries_from_db(
                session=session,
                batch_size=batch_size,
                project_id=project_id,
                min_text_len=min_text_len,
                log=logger
            ),
            new_items=gen_academic_entries(new_items()),
            vectoriser=vectoriser,
            max_slop=max_slop,
            batch_size=batch_size)

    logger.debug('  -> initialising duplicate detection index...')
    await index.init()

    logger.info('Done building the index! Next, I\'m going through new items and adding them!')

    for item in new_items():
        logger.info(f'Importing AcademicItem with doi {item.doi} and title "{item.title}"')

        # remove empty entries from the meta-data field
        item.meta = get_cleaned_meta_field(item)

        # ensure the project id is set
        item.project_id = project_id

        # ensure we have a title_slug
        if item.title_slug is None or len(item.title_slug) == 0:
            item.title_slug = str_to_title_slug(item.title)

        txt = itm2txt(item)
        existing_id: str | None = None
        if len(txt) > min_text_len:
            existing_id = index.test(ItemEntry(item_id=str(item.item_id), text=txt))

        if existing_id is not None:
            logger.debug(f'  -> Text lookup found duplicate: {existing_id}')
        else:
            duplicates = await find_duplicates(item=item,
                                               project_id=str(project_id),
                                               check_tslug=True,
                                               check_tslug_advanced=True,
                                               check_doi=True,
                                               check_wos_id=True,
                                               check_scopus_id=True,
                                               check_oa_id=True,
                                               check_pubmed_id=True,
                                               check_dimensions_id=True,
                                               check_s2_id=True,
                                               session=session)
            if duplicates is not None and len(duplicates):
                existing_id = str(duplicates[0].item_id)
                logger.debug(f'  -> There are at least {len(duplicates)}; I will probably use {existing_id}')

        try:
            if existing_id is not None:
                item_id = existing_id
                if not dry_run:
                    if item.item_id is None:
                        item.item_id = uuid.uuid4()
                    await duplicate_insertion(orig_item_id=existing_id,
                                              import_id=import_id,
                                              new_item=item,
                                              trust_new_authors=trust_new_authors,
                                              trust_new_keywords=trust_new_keywords,
                                              session=session)
            else:
                item_id = str(uuid.uuid4())
                if dry_run:
                    logger.debug('  -> I will create a new AcademicItem!')
                else:
                    logger.debug(f' -> Creating new item with ID {item_id}!')
                    item.item_id = item_id
                    session.add(AcademicItem(**item.model_dump()))
                    await session.commit()

            if dry_run:
                logger.debug('  -> I will create an m2m entry.')
            else:
                item_ids.append(item_id)
                stmt_m2m = insert(m2m_import_item_table) \
                    .values(item_id=item_id, import_id=import_id, type=M2MImportItemType.explicit)
                try:
                    await session.execute(stmt_m2m)
                    await session.commit()
                    logger.debug(' -> Added many-to-many relationship for import/item')
                except IntegrityError:
                    logger.debug(f' -> M2M_i2i already exists, ignoring {import_id} <-> {item_id}')
                    await session.rollback()

        except (UniqueViolation, IntegrityError, OperationalError) as e:
            logger.exception(e)
            await session.rollback()

    return import_id, item_ids


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

    from nacsos_data.util.academic.readers.wos import read_wos_file
    if len(sources) == 0:
        raise ValueError('Missing source files!')

    logger = logging.getLogger('import_wos_file') if logger is None else logger

    def from_sources() -> Generator[AcademicItemModel, None, None]:
        for source in sources:
            for itm in read_wos_file(filepath=str(source), project_id=project_id):
                itm.item_id = uuid.uuid4()
                yield itm

    logger.info(f'Importing articles from web of science files: {sources}')
    if import_id is None:
        raise ValueError('Import ID is not set!')
    if project_id is None:
        raise ValueError('Project ID is not set!')
    db_engine = get_engine_async(conf_file=str(db_config))

    await import_academic_items(
        db_engine=db_engine,
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
        logger=logger
    )


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

    from nacsos_data.util.academic.readers.scopus import read_scopus_csv_file
    if len(sources) == 0:
        raise ValueError('Missing source files!')

    logger = logging.getLogger('import_scopus_csv') if logger is None else logger

    def from_sources() -> Generator[AcademicItemModel, None, None]:
        for source in sources:
            for itm in read_scopus_csv_file(filepath=str(source), project_id=project_id):
                itm.item_id = uuid.uuid4()
                yield itm

    logger.info(f'Importing articles from scopus CSV files: {sources}')
    if import_id is None:
        raise ValueError('Import ID is not set!')
    if project_id is None:
        raise ValueError('Project ID is not set!')
    db_engine = get_engine_async(conf_file=str(db_config))

    await import_academic_items(
        db_engine=db_engine,
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
        logger=logger
    )


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
    if import_id is None:
        raise ValueError('Import ID is not set!')
    if project_id is None:
        raise ValueError('Project ID is not set!')
    db_engine = get_engine_async(conf_file=str(db_config))

    await import_academic_items(
        db_engine=db_engine,
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
        logger=logger
    )


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
    from nacsos_data.util.academic.readers.openalex import generate_items_from_openalex
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
    if import_id is None:
        raise ValueError('Import ID is not set!')
    if project_id is None:
        raise ValueError('Project ID is not set!')
    db_engine = get_engine_async(conf_file=str(db_config))

    await import_academic_items(
        db_engine=db_engine,
        project_id=project_id,
        new_items=from_source,
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
        logger=logger
    )


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
    from nacsos_data.models.openalex.solr import WorkSolr
    from nacsos_data.util.academic.readers.openalex import translate_doc, translate_work

    logger = logging.getLogger('import_openalex_files') if logger is None else logger

    def from_sources() -> Generator[AcademicItemModel, None, None]:
        for source in sources:
            with open(source, 'r') as f:
                for line in f:
                    itm = translate_work(translate_doc(WorkSolr.model_validate_json(line)))
                    itm.item_id = uuid.uuid4()
                    yield itm

    logger.info(f'Importing articles (WorkSolr-formatted) from files: {sources}')
    if import_id is None:
        raise ValueError('Import ID is not set!')
    if project_id is None:
        raise ValueError('Project ID is not set!')
    db_engine = get_engine_async(conf_file=str(db_config))

    await import_academic_items(
        db_engine=db_engine,
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
        logger=logger
    )
