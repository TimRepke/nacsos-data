import uuid
import logging
import tempfile

from pathlib import Path
from typing import Generator, IO
from collections import defaultdict

from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401
from psycopg.errors import UniqueViolation, OperationalError
from sklearn.feature_extraction.text import CountVectorizer

from ...db import DatabaseEngineAsync, get_engine_async
from ...db.crud.imports import get_or_create_import
from ...db.crud.items.academic import (
    AcademicItemGenerator,
    read_item_entries_from_db,
    gen_academic_entries,
    read_item_ids_for_import,
    read_known_ids_map,
    IdField
)
from ...db.schemas import AcademicItem, m2m_import_item_table
from ...models.items import AcademicItemModel
from ...models.imports import M2MImportItemType
from ...models.openalex.solr import DefType, SearchField, OpType
from .. import gather_async
from ..text import tokenise_item, extract_vocabulary, itm2txt
from ..duplicate import ItemEntry, DuplicateIndex, MilvusDuplicateIndex
from .clean import get_cleaned_meta_field
from .duplicate import str_to_title_slug, find_duplicates, duplicate_insertion

ID_FIELDS: list[IdField] = ['openalex_id', 's2_id', 'scopus_id', 'wos_id', 'pubmed_id', 'dimensions_id']


def _read_buffered_items(fp: IO[str]) -> Generator[AcademicItemModel, None, None]:
    fp.seek(0)
    for line in fp:
        yield AcademicItemModel.model_validate_json(line)


def _check_known_identifiers(item: AcademicItemModel,
                             known_ids: dict[str, dict[str, str]],
                             imported_item_ids: set[str],
                             logger: logging.Logger) -> str | None | bool:
    for id_field in ID_FIELDS:  # check each item
        identifier = getattr(item, id_field)
        item_id = known_ids[id_field].get(identifier, None)

        if identifier is not None and item_id is not None:
            if item_id not in imported_item_ids:
                logger.debug(' -> Found ID match and adding it to import/item m2m buffer')
                return item_id
            else:
                logger.debug(' -> Found ID match but not adding it to m2m buffer (already exists)')
                return None
    return False


async def _find_id_duplicates(
        session: AsyncSession,
        new_items: AcademicItemGenerator,
        fp: IO[str],
        import_id: str,
        project_id: str,
        logger: logging.Logger) -> tuple[int, dict[str, int], set[str], set[str]]:
    logger.info('Fetching all known IDs from current project...')
    known_ids: dict[str, dict[str, str]]
    known_ids = {
        id_field: await read_known_ids_map(session=session, project_id=project_id, field=id_field)
        for id_field in ID_FIELDS
    }

    # Fetch item_ids already in the import (in case the import failed at some point or is continued mid-way)
    imported_item_ids = set(await gather_async(read_item_ids_for_import(session=session, import_id=import_id)))

    # Set of item_ids for which we need to add m2m tuples later
    m2m_buffer: set[str] = set()
    # Accumulator for our vocabulary
    token_counts: defaultdict[str, int] = defaultdict(int)
    n_unknown_items = 0

    logger.info('Checking if there are any known identifiers in the new data...')
    for item in new_items():  # Iterate all new items
        # Check if we know this new item via some trusted identifier (e.g. openalex_id)
        known_item_id = _check_known_identifiers(item, known_ids, imported_item_ids, logger)

        # We know this new item (via an ID) and just need to add an m2m

        # We don't know this new item, add it to our buffer file and extend vocabulary
        if known_item_id is False:
            for tok in tokenise_item(item, lowercase=True):
                token_counts[tok] += 1
            fp.write(item.model_dump_json() + '\n')
            n_unknown_items += 1

        # We found a match!
        elif known_item_id is not None and type(known_item_id) is str:
            if known_item_id not in imported_item_ids:
                m2m_buffer.add(known_item_id)
                imported_item_ids.add(known_item_id)

    # return number of items we need to check for duplicates, vocabulary, updated set of seen item_ids, and the m2m buffer
    return n_unknown_items, token_counts, imported_item_ids, m2m_buffer


async def _insert_m2m(session: AsyncSession, item_id: str, import_id: str, dry_run: bool, logger: logging.Logger) -> None:
    if dry_run:
        logger.debug(' [DRY-RUN] -> Added many-to-many relationship for import/item')
    else:
        stmt_m2m = insert(m2m_import_item_table).values(item_id=item_id, import_id=import_id, type=M2MImportItemType.explicit)
        await session.execute(stmt_m2m)
        await session.flush()
        logger.debug(' -> Added many-to-many relationship for import/item')


async def _find_duplicate(session: AsyncSession,
                          item: AcademicItemModel,
                          project_id: str,
                          min_text_len: int,
                          index: DuplicateIndex | MilvusDuplicateIndex,
                          logger: logging.Logger) -> str | None:
    txt = itm2txt(item)
    if len(txt) > min_text_len:
        existing_id = index.test(ItemEntry(item_id=str(item.item_id), text=txt))
        if existing_id is not None:
            logger.debug(f'  -> Text lookup found duplicate: {existing_id}')
            return existing_id

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
        return existing_id

    logger.debug('  -> No duplicate found.')
    return None


def _ensure_clean_item(item: AcademicItemModel, project_id: str) -> AcademicItemModel:
    # remove empty entries from the meta-data field
    item.meta = get_cleaned_meta_field(item)

    # ensure the project id is set
    item.project_id = project_id

    # ensure we have a title_slug
    if item.title_slug is None or len(item.title_slug) == 0:
        item.title_slug = str_to_title_slug(item.title)

    return item


async def _insert_item(session: AsyncSession,
                       item: AcademicItemModel,
                       import_id: str,
                       existing_id: str | None,
                       trust_new_authors: bool,
                       trust_new_keywords: bool,
                       dry_run: bool,
                       logger: logging.Logger) -> str:
    if existing_id is None:
        item_id = str(uuid.uuid4())
        if dry_run:
            logger.debug(f'  [DRY-RUN] -> Creating new item with ID {item_id}!')
            return item_id

        logger.debug(f' -> Creating new item with ID {item_id}!')
        item.item_id = item_id
        session.add(AcademicItem(**item.model_dump()))
        await session.flush()
        return item_id

    if item.item_id is None:
        item.item_id = uuid.uuid4()

    if dry_run:
        logger.debug(f'  [DRY-RUN] -> Creating variant for item_id {existing_id} with variant_id {item.item_id}!')
        return existing_id

    logger.debug(f'  -> Creating variant for item_id {existing_id} with variant_id {item.item_id}!')
    await duplicate_insertion(orig_item_id=existing_id,
                              import_id=import_id,
                              new_item=item,
                              trust_new_authors=trust_new_authors,
                              trust_new_keywords=trust_new_keywords,
                              session=session)
    return existing_id


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
        max_features: int = 5000,
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
    :param max_features: Maximum number of features for the vectorizer
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

    with tempfile.NamedTemporaryFile('w+') as duplicate_buffer:
        async with (db_engine.session() as session):
            # Get the import and figure out what ids to deduplicate on, based on import type
            import_orm = await get_or_create_import(session=session,
                                                    project_id=project_id,
                                                    import_id=import_id,
                                                    user_id=user_id,
                                                    import_name=import_name,
                                                    description=description,
                                                    i_type='script')
            import_id = str(import_orm.import_id)

            logger.info('Checking new items for obvious ID-based duplicates...')
            n_unknown_items, token_counts, imported_item_ids, m2m_buffer = await _find_id_duplicates(
                session=session,
                project_id=str(project_id),
                import_id=import_id,
                new_items=new_items,
                logger=logger,
                fp=duplicate_buffer
            )
            logger.info(f'Found {n_unknown_items:,} unknown items and {len(m2m_buffer):,} duplicates in first pass.')

        index: MilvusDuplicateIndex | None = None
        if n_unknown_items > 0:
            logger.debug('Constructing vocabulary...')
            vocabulary = extract_vocabulary(token_counts, min_count=1, max_features=max_features)

            if vectoriser is None:
                vectoriser = CountVectorizer(vocabulary=vocabulary)

            del token_counts  # clean up term counts to save RAM

            logger.debug('Constructing ANN index...')
            async with (db_engine.session() as session):
                index = MilvusDuplicateIndex(
                    existing_items=read_item_entries_from_db(
                        session=session,
                        batch_size=batch_size,
                        project_id=project_id,
                        min_text_len=min_text_len,
                        log=logger
                    ),
                    new_items=gen_academic_entries(_read_buffered_items(duplicate_buffer)),
                    project_id=project_id,
                    vectoriser=vectoriser,
                    max_slop=max_slop,
                    batch_size=batch_size)

                logger.debug('  -> initialising duplicate detection index...')
                await index.init()

        logger.info('Finished pre-processing and index building.')
        logger.info('Proceeding to insert new items and creating m2m tuples...')
        async with (db_engine.session() as session):
            logger.info(f'Inserting {len(m2m_buffer):,} buffered m2m relations...')
            for item_id in m2m_buffer:
                await _insert_m2m(session=session, item_id=item_id, import_id=import_id, logger=logger, dry_run=dry_run)

            if n_unknown_items == 0 or index is None:
                logger.info('No unknown items found, ending here!')
                return import_id, list(imported_item_ids)

            logger.info(f'Inserting (maybe) {n_unknown_items:,} buffered duplicate candidates...')
            index.client.load_collection(index.collection_name)
            for item in _read_buffered_items(duplicate_buffer):
                try:
                    logger.info(f'Importing AcademicItem with doi {item.doi} and title "{item.title}"')

                    # Make sure the item fields are complete and clean
                    item = _ensure_clean_item(item, project_id=str(project_id))

                    # Search for duplicates in the index and the database
                    existing_id = await _find_duplicate(session=session, item=item, project_id=str(project_id),
                                                        min_text_len=min_text_len, index=index, logger=logger)

                    # Insert a new item or an item variant
                    item_id = await _insert_item(session=session, item=item, existing_id=existing_id, import_id=import_id,
                                                 trust_new_authors=trust_new_authors, trust_new_keywords=trust_new_keywords,
                                                 dry_run=dry_run, logger=logger)

                    # Add many-to-many relation to import
                    if item_id not in imported_item_ids:
                        imported_item_ids.add(item_id)
                        await _insert_m2m(session=session, item_id=item_id, import_id=import_id, dry_run=dry_run, logger=logger)

                except (UniqueViolation, IntegrityError, OperationalError) as e:
                    logger.exception(e)
                    await session.rollback()

    index.client.drop_collection(index.collection_name)

    return import_id, list(imported_item_ids)


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
