import uuid
import logging
import tempfile

from pathlib import Path
from typing import Generator, IO
from collections import defaultdict

from sqlalchemy import text, update
from sqlalchemy.dialects.postgresql import insert as insert_pg
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401
from psycopg.errors import UniqueViolation, OperationalError
from sklearn.feature_extraction.text import CountVectorizer

from ...db import DatabaseEngineAsync, get_engine_async
from ...db.crud.imports import get_or_create_import, set_session_mutex
from ...db.crud.items.academic import (
    AcademicItemGenerator,
    read_item_entries_from_db,
    gen_academic_entries,
    read_known_ids_map,
    IdField
)
from ...db.schemas import AcademicItem, m2m_import_item_table
from ...db.schemas.imports import ImportRevision
from ...models.items import AcademicItemModel, ItemEntry
from ...models.imports import M2MImportItemType, ImportRevisionModel
from ...models.openalex.solr import DefType, SearchField, OpType
from .. import elapsed_timer
from ..text import tokenise_item, extract_vocabulary, itm2txt
from ..duplicate import MilvusDuplicateIndex, PynndescentDuplicateIndex
from .clean import get_cleaned_meta_field
from .duplicate import str_to_title_slug, find_duplicates, duplicate_insertion

ID_FIELDS: list[IdField] = ['openalex_id', 's2_id', 'scopus_id', 'wos_id', 'pubmed_id', 'dimensions_id']


def _read_buffered_items(fp: IO[str]) -> Generator[AcademicItemModel, None, None]:
    fp.seek(0)
    for line in fp:
        yield AcademicItemModel.model_validate_json(line)


def _check_known_identifiers(item: AcademicItemModel,
                             known_ids: dict[str, dict[str, str]],
                             logger: logging.Logger) -> str | bool:
    for id_field in ID_FIELDS:  # check each item
        identifier = getattr(item, id_field)
        item_id = known_ids[id_field].get(identifier, None)

        if identifier is not None and item_id is not None:
            logger.debug(' -> Found ID match and adding it to import/item m2m buffer')
            return item_id

    return False


async def _find_id_duplicates(
        session: AsyncSession,
        new_items: AcademicItemGenerator,
        fp: IO[str],
        project_id: str,
        logger: logging.Logger) -> tuple[int, int, dict[str, int], set[str]]:
    with elapsed_timer(logger, 'Fetching all known IDs from current project'):
        known_ids: dict[str, dict[str, str]]
        known_ids = {
            id_field: await read_known_ids_map(session=session, project_id=project_id, field=id_field)
            for id_field in ID_FIELDS
        }

    # Set of item_ids for which we need to add m2m tuples later
    m2m_buffer: set[str] = set()
    # Accumulator for our vocabulary
    token_counts: defaultdict[str, int] = defaultdict(int)
    n_unknown_items = 0
    n_new_items = 0

    with elapsed_timer(logger, 'Checking if there are any known identifiers in the new data'):
        for item in new_items():  # Iterate all new items
            n_new_items += 1
            # Check if we know this new item via some trusted identifier (e.g. openalex_id)
            known_item_id = _check_known_identifiers(item, known_ids, logger)

            # We don't know this new item, add it to our buffer file and extend vocabulary
            if known_item_id is False:
                for tok in tokenise_item(item, lowercase=True):
                    token_counts[tok] += 1
                fp.write(item.model_dump_json() + '\n')
                n_unknown_items += 1
            elif known_item_id is True:  # never happens, just to appease mypy
                pass
            # We found a match!
            else:
                m2m_buffer.add(known_item_id)

    # return number of items we need to check for duplicates, vocabulary, updated set of seen item_ids, and the m2m buffer
    return n_unknown_items, n_new_items, token_counts, m2m_buffer


async def _upsert_m2m(session: AsyncSession, item_id: str, import_id: str, latest_revision: int,
                      dry_run: bool, logger: logging.Logger) -> None:
    if dry_run:
        logger.debug(' [DRY-RUN] -> Added many-to-many relationship for import/item')
    else:
        stmt_m2m = (insert_pg(m2m_import_item_table)
                    .values(item_id=item_id, import_id=import_id, type=M2MImportItemType.explicit,
                            first_revision=latest_revision, latest_revision=latest_revision)
                    .on_conflict_do_update(constraint='m2m_import_item_pkey',
                                           set_={
                                               'latest_revision': latest_revision,
                                           }))
        await session.execute(stmt_m2m)
        await session.flush()
        logger.debug(' -> Added many-to-many relationship for import/item')


async def _find_duplicate(session: AsyncSession,
                          item: AcademicItemModel,
                          project_id: str,
                          min_text_len: int,
                          index: PynndescentDuplicateIndex | MilvusDuplicateIndex,
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
                       dry_run: bool,
                       logger: logging.Logger) -> tuple[str, bool]:
    """
    Returns the item_id of the item that was inserted. This might be the existing ID or a new one.
    The second return value is
       False: New item was created (no variant, yet)
       True: Variant was created

    :param session:
    :param item:
    :param import_id:
    :param existing_id:
    :param dry_run:
    :param logger:
    :return: tuple of item_id and variant insertion
    """
    if existing_id is None:
        item_id = str(uuid.uuid4())
        if dry_run:
            logger.debug(f'  [DRY-RUN] -> Creating new item with ID {item_id}!')
            return item_id, False

        logger.debug(f' -> Creating new item with ID {item_id}!')
        item.item_id = item_id
        session.add(AcademicItem(**item.model_dump()))
        await session.flush()
        return item_id, False

    if item.item_id is None:
        item.item_id = uuid.uuid4()

    if dry_run:
        logger.debug(f'  [DRY-RUN] -> Creating variant for item_id {existing_id} with variant_id {item.item_id}!')
        return existing_id, True

    logger.debug(f'  -> Creating variant for item_id {existing_id} with variant_id {item.item_id}!')
    has_changes = await duplicate_insertion(orig_item_id=existing_id,
                                            import_id=import_id,
                                            new_item=item,
                                            session=session)
    return existing_id, has_changes


async def _get_latest_revision_counter(session: AsyncSession, import_id: str | uuid.UUID) -> int:
    latest_revision: int = (await session.execute(  # type: ignore[assignment]
        text('SELECT COALESCE(MAX("import_revision_counter"), 0) as latest_revision '
             'FROM import_revision '
             'WHERE import_id=:import_id;'),
        {'import_id': import_id}
    )).scalar()
    return latest_revision


async def _get_latest_revision(session: AsyncSession, import_id: str | uuid.UUID) -> ImportRevisionModel | None:
    rslt = (await session.execute(
        text('SELECT * FROM import_revision WHERE import_id=:import_id ORDER BY import_revision_counter DESC LIMIT 1;'),
        {'import_id': import_id}
    )).mappings().one_or_none()
    if rslt:
        return ImportRevisionModel(**rslt)
    return None


def _revision_required(num_new_items: int | None, last_revision: ImportRevisionModel | None,
                       min_update_size: int | None, logger: logging.Logger) -> bool:
    if (min_update_size is not None
            and num_new_items is not None
            and last_revision is not None
            and last_revision.num_items_retrieved is not None
            and abs(last_revision.num_items_retrieved - num_new_items) < min_update_size):
        logger.warning(f'Expected a difference of {min_update_size} between {num_new_items:,} new items in query and '
                       f'{last_revision.num_items_retrieved:,} items in latest revision. Ending here!')
        return False
    return True


async def _update_statistics(session: AsyncSession,
                             project_id: str | uuid.UUID,
                             import_id: str | uuid.UUID,
                             revision_id: str | uuid.UUID,
                             latest_revision: int,
                             num_new_items: int,
                             num_updated: int,
                             logger: logging.Logger) -> None:
    with elapsed_timer(logger, 'Updating revision stats...'):
        num_items = (await session.execute(text('SELECT count(1) FROM m2m_import_item WHERE import_id = :import_id'),
                                           {'import_id': import_id})).scalar()
        num_items_new = (await session.execute(text('SELECT count(1) '
                                                    'FROM m2m_import_item '
                                                    'WHERE first_revision = :rev AND import_id=:import_id'),
                                               {'rev': latest_revision, 'import_id': import_id})).scalar()
        num_items_removed = (await session.execute(text('SELECT count(1) '
                                                        'FROM m2m_import_item '
                                                        'WHERE latest_revision = :rev AND import_id=:import_id'),
                                                   {'rev': latest_revision - 1, 'import_id': import_id})).scalar()
        revision_stats = {
            'num_items': num_items,
            'num_items_retrieved': num_new_items,
            'num_items_new': num_items_new,
            'num_items_updated': num_updated,
            'num_items_removed': num_items_removed,
        }
        logger.info(f'Setting new revision stats: {revision_stats}')
        await session.execute(update(ImportRevision)
                              .where(ImportRevision.import_revision_id==revision_id)
                              .values(**revision_stats))
        await session.commit()

    return None


async def import_academic_items(
        db_engine: DatabaseEngineAsync,
        project_id: str | uuid.UUID,
        new_items: AcademicItemGenerator,
        import_name: str | None = None,
        import_id: str | uuid.UUID | None = None,
        pipeline_task_id: str | None = None,
        user_id: str | uuid.UUID | None = None,
        description: str | None = None,
        vectoriser: CountVectorizer | None = None,
        min_update_size: int | None = None,
        num_new_items: int | None = None,
        max_slop: float = 0.05,
        min_text_len: int = 300,
        batch_size: int = 2000,
        max_features: int = 5000,
        dry_run: bool = True,
        logger: logging.Logger | None = None
) -> tuple[str, int | None]:
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
    :param min_update_size: Minimum difference in number of items between revisions
                            if None, will always create a new revision
                            if difference between `num_new_items` is lower than `min_update_size`, will not create new revision
    :param num_new_items: Number of new items yielded by `new_items` generator; if empty, will derive this number from the generator
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
    :return: import_id, latest_revision_num (or None if no action taken)
    """
    if logger is None:
        logger = logging.getLogger('nacsos_data.util.academic.import')

    if project_id is None:
        raise AttributeError('You have to provide a project ID!')

    with tempfile.NamedTemporaryFile('w+') as duplicate_buffer:
        async with db_engine.session() as session:  # type: AsyncSession
            await set_session_mutex(session, project_id=project_id, lock=True)

            # Get the import and figure out what ids to deduplicate on, based on import type
            import_orm = await get_or_create_import(session=session,
                                                    project_id=project_id,
                                                    import_id=import_id,
                                                    user_id=user_id,
                                                    import_name=import_name,
                                                    description=description,
                                                    i_type='script')
            import_id = str(import_orm.import_id)

            revision_id = str(uuid.uuid4())
            last_revision = await _get_latest_revision(session=session, import_id=import_id)
            latest_revision = 1
            if last_revision is not None:
                latest_revision = last_revision.import_revision_counter + 1

                # Check if we should even create a new revision based on the difference in the number of query results
                if not _revision_required(num_new_items=num_new_items, last_revision=last_revision,
                                          min_update_size=min_update_size, logger=logger):
                    # Free our import mutex
                    await set_session_mutex(session, project_id=project_id, lock=False)
                    # Return without committing anything (note, that freeing mutex will roll back session!
                    return import_id, None

            # Create a new revision
            session.add(ImportRevision(
                import_revision_id=revision_id,
                import_id=import_id,
                import_revision_counter=latest_revision,
                pipeline_task_id=pipeline_task_id,
            ))
            await session.commit()  # Note, committing instead of flushing here, so this is persisted for reference even on error

            with elapsed_timer(logger, 'Checking new items for obvious ID-based duplicates'):
                n_unknown_items, num_new_items, token_counts, m2m_buffer = await _find_id_duplicates(
                    session=session,
                    project_id=str(project_id),
                    new_items=new_items,
                    logger=logger,
                    fp=duplicate_buffer
                )
            logger.info(f'Found {n_unknown_items:,} unknown items and {len(m2m_buffer):,} duplicates in first pass.')

            # Check if we should even create a new revision based on the difference in the number of query results.
            # This is repeating the previous check in case the `num_new_items` parameter was left empty before.
            if not _revision_required(num_new_items=num_new_items, last_revision=last_revision,
                                      min_update_size=min_update_size, logger=logger):
                # Free our import mutex (note, that freeing mutex will roll back session!)
                await set_session_mutex(session, project_id=project_id, lock=False)
                # Return without committing anything
                return import_id, None

        index: MilvusDuplicateIndex | None = None
        if n_unknown_items > 0:
            with elapsed_timer(logger, f'Constructing vocabulary from {len(token_counts):,} `token_counts`'):
                vocabulary = extract_vocabulary(token_counts, min_count=1, max_features=max_features)
                del token_counts  # clean up term counts to save RAM

            if vectoriser is None:
                with elapsed_timer(logger, f'Setting up vectorizer with {len(vocabulary):,} tokens in the vocabulary'):
                    vectoriser = CountVectorizer(vocabulary=vocabulary)

            logger.debug('Constructing Milvus index...')
            async with db_engine.session() as session:  # type: AsyncSession
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

                with elapsed_timer(logger, '  -> initialising duplicate detection index...'):
                    await index.init()

        logger.info('Finished pre-processing and index building.')
        logger.info('Proceeding to insert new items and creating m2m tuples...')
        num_updated = 0
        async with db_engine.session() as session:  # type: AsyncSession
            with elapsed_timer(logger, f'Inserting {len(m2m_buffer):,} buffered m2m relations'):
                for item_id in m2m_buffer:
                    await _upsert_m2m(session=session, item_id=item_id, import_id=import_id, latest_revision=latest_revision,
                                      logger=logger, dry_run=dry_run)

            if n_unknown_items == 0 or index is None:
                logger.info('No unknown items found, ending here!')
                # Commit all changes
                await session.commit()

                # Updating revision stats
                await _update_statistics(session=session, project_id=project_id, import_id=import_id, revision_id=revision_id,
                                         latest_revision=latest_revision, num_new_items=num_new_items, num_updated=num_updated,
                                         logger=logger)

                # Free our import mutex
                await set_session_mutex(session, project_id=project_id, lock=False)

                # Return to caller
                return import_id, latest_revision

            with elapsed_timer(logger, f'Loading milvus collection "{index.collection_name}"'):
                index.client.load_collection(index.collection_name)

            logger.info(f'Inserting (maybe) {n_unknown_items:,} buffered duplicate candidates...')
            for item in _read_buffered_items(duplicate_buffer):
                try:
                    async with session.begin_nested():
                        with elapsed_timer(logger, f'Importing AcademicItem with doi {item.doi} and title "{item.title}"'):
                            # Make sure the item fields are complete and clean
                            item = _ensure_clean_item(item, project_id=str(project_id))

                            # Search for duplicates in the index and the database
                            existing_id = await _find_duplicate(session=session, item=item, project_id=str(project_id),
                                                                min_text_len=min_text_len, index=index, logger=logger)

                            # Insert a new item or an item variant
                            item_id, has_changes = await _insert_item(session=session, item=item, existing_id=existing_id,
                                                                      import_id=import_id, dry_run=dry_run, logger=logger)
                            num_updated += has_changes

                            # UPSERT m2m
                            await _upsert_m2m(session=session, item_id=item_id, import_id=import_id, latest_revision=latest_revision,
                                              logger=logger, dry_run=dry_run)

                except (UniqueViolation, IntegrityError, OperationalError) as e:
                    logger.exception(e)

            # All done, commit and finalise import transaction.
            logger.info('Finally committing all changes to the database!')
            await session.commit()

    with elapsed_timer(logger, 'Cleaning up milvus!'):
        index.client.drop_collection(index.collection_name)

    async with db_engine.session() as session:  # type: AsyncSession
        await _update_statistics(session=session, project_id=project_id,import_id=import_id,revision_id=revision_id,
                           latest_revision=latest_revision, num_new_items=num_new_items, num_updated=num_updated, logger=logger)
        # Free our import mutex
        await set_session_mutex(session, project_id=project_id, lock=False)

    logger.info('Import complete, returning to initiator!')
    return import_id, latest_revision


async def import_wos_files(sources: list[Path],
                           db_config: Path,
                           project_id: str | None = None,
                           import_id: str | None = None,
                           pipeline_task_id: str | None = None,
                           min_update_size: int | None = None,
                           num_new_items: int | None = None,
                           logger: logging.Logger | None = None) -> tuple[str, int | None]:
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

    return await import_academic_items(
        db_engine=db_engine,
        project_id=project_id,
        new_items=from_sources,
        min_update_size=min_update_size,
        num_new_items=num_new_items,
        import_name=None,
        description=None,
        user_id=None,
        import_id=import_id,
        pipeline_task_id=pipeline_task_id,
        vectoriser=None,
        max_slop=0.05,
        batch_size=5000,
        dry_run=False,
        logger=logger
    )


async def import_scopus_csv_file(sources: list[Path],
                                 db_config: Path,
                                 project_id: str | None = None,
                                 import_id: str | None = None,
                                 pipeline_task_id: str | None = None,
                                 min_update_size: int | None = None,
                                 num_new_items: int | None = None,
                                 logger: logging.Logger | None = None) -> tuple[str, int | None]:
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

    return await import_academic_items(
        db_engine=db_engine,
        project_id=project_id,
        new_items=from_sources,
        min_update_size=min_update_size,
        num_new_items=num_new_items,
        import_name=None,
        description=None,
        user_id=None,
        import_id=import_id,
        pipeline_task_id=pipeline_task_id,
        vectoriser=None,
        max_slop=0.05,
        batch_size=5000,
        dry_run=False,
        logger=logger
    )


async def import_academic_db(sources: list[Path],
                             db_config: Path,
                             project_id: str | None = None,
                             import_id: str | None = None,
                             pipeline_task_id: str | None = None,
                             min_update_size: int | None = None,
                             num_new_items: int | None = None,
                             logger: logging.Logger | None = None) -> tuple[str, int | None]:
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

    return await import_academic_items(
        db_engine=db_engine,
        project_id=project_id,
        new_items=from_sources,
        min_update_size=min_update_size,
        num_new_items=num_new_items,
        import_name=None,
        description=None,
        user_id=None,
        import_id=import_id,
        pipeline_task_id=pipeline_task_id,
        vectoriser=None,
        max_slop=0.05,
        batch_size=5000,
        dry_run=False,
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
                          pipeline_task_id: str | None = None,
                          min_update_size: int | None = None,
                          logger: logging.Logger | None = None) -> tuple[str, int | None]:
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
    from nacsos_data.util.academic.readers.openalex import generate_items_from_openalex, get_count_from_openalex
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

    num_new_items = None
    # In case we are going to check it, fetch the item count
    if min_update_size is not None:
        num_new_items = (
            await get_count_from_openalex(query=query, openalex_endpoint=openalex_url, op=op, field=field, def_type=def_type)
        ).num_found

    db_engine = get_engine_async(conf_file=str(db_config))

    return await import_academic_items(
        db_engine=db_engine,
        project_id=project_id,
        new_items=from_source,
        min_update_size=min_update_size,
        num_new_items=num_new_items,
        import_name=None,
        description=None,
        user_id=None,
        import_id=import_id,
        pipeline_task_id=pipeline_task_id,
        vectoriser=None,
        max_slop=0.05,
        batch_size=5000,
        dry_run=False,
        logger=logger
    )


async def import_openalex_files(sources: list[Path],
                                db_config: Path,
                                project_id: str | None = None,
                                import_id: str | None = None,
                                pipeline_task_id: str | None = None,
                                min_update_size: int | None = None,
                                num_new_items: int | None = None,
                                logger: logging.Logger | None = None) -> tuple[str, int | None]:
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

    return await import_academic_items(
        db_engine=db_engine,
        project_id=project_id,
        new_items=from_sources,
        min_update_size=min_update_size,
        num_new_items=num_new_items,
        import_name=None,
        description=None,
        user_id=None,
        import_id=import_id,
        pipeline_task_id=pipeline_task_id,
        vectoriser=None,
        max_slop=0.05,
        batch_size=5000,
        dry_run=False,
        logger=logger
    )
