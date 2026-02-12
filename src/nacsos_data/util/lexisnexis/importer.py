import logging
import uuid
from collections import defaultdict
from typing import Generator, AsyncGenerator, TypeAlias, Callable

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401
from psycopg.errors import UniqueViolation, OperationalError

from nacsos_data.db.engine import DatabaseEngineAsync
from nacsos_data.db.schemas import Item, LexisNexisItem, LexisNexisItemSource

from .. import elapsed_timer
from ..duplicate import MilvusDuplicateIndex
from ..text import extract_vocabulary, tokenise_item
from ...db.crud.imports import get_or_create_import, set_session_mutex, get_latest_revision, upsert_m2m, update_revision_statistics
from ...db.schemas.imports import ImportRevision
from ...models.items import FullLexisNexisItemModel, ItemEntry


from sklearn.feature_extraction.text import CountVectorizer

LexisNexisItemGenerator: TypeAlias = Callable[[], Generator[FullLexisNexisItemModel, None, None]]


async def read_known_ids(
    session: AsyncSession,
    project_id: str | uuid.UUID,
    logger: logging.Logger,
    batch_size: int = 5000,
    allow_empty_text: bool = False,
) -> dict[str, str]:
    txt_where = '' if allow_empty_text else 'AND i.text IS NOT NULL'

    rslt = (
        (
            await session.stream(
                sa.text(f"""
                    SELECT lis.lexis_id, lis.item_id::text
                    FROM lexis_item_source lis
                    JOIN item i ON lis.item_id = i.item_id
                    WHERE i.project_id = :project_id {txt_where};
                """),
                {'project_id': project_id},
            )
        )
        .mappings()
        .partitions(batch_size)
    )

    ids: dict[str, str] = {}
    async for batch in rslt:
        logger.debug(f'Received batch with {len(batch)} entries.')
        ids |= {record['lexis_id']: record['item_id'] for record in batch}
    return ids


async def _item_entries_from_db(
    session: AsyncSession,
    project_id: str | uuid.UUID,
    min_text_len: int,
    max_text_len: int,
    logger: logging.Logger,
    batch_size: int = 500,
) -> AsyncGenerator[list[ItemEntry], None]:
    stmt = (
        sa.select(Item.text, Item.item_id)
        .where(Item.project_id == project_id, Item.text.isnot(None), sa.func.char_length(Item.text) > min_text_len)
        .execution_options(yield_per=batch_size)
    )
    rslt = (await session.stream(stmt)).mappings().partitions()

    async for batch in rslt:
        logger.debug(f'Received batch with {len(batch)} entries.')
        yield [ItemEntry(item_id=str(record['item_id']), text=record['text'][:max_text_len]) for record in batch]


def _filtered_entries(
    new_items: LexisNexisItemGenerator,
    known_ids: dict[str, str],
    min_text_len: int,
    max_text_len: int,
) -> Generator[ItemEntry, None, None]:
    for item in new_items():
        if item.text is None or len(item.text) < min_text_len:
            continue

        for source in item.sources or []:
            if source.lexis_id in known_ids:
                break
        else:
            yield ItemEntry(item_id=str(item.item_id), text=item.text[:max_text_len])


async def find_by_source(session: AsyncSession, project_id: str | uuid.UUID, lexis_id: str) -> tuple[uuid.UUID, uuid.UUID] | None:
    stmt = sa.text("""
        SELECT lis.item_id, lis.item_source_id
        FROM lexis_item_source lis
        JOIN item i ON lis.item_id = i.item_id
        WHERE i.project_id = :project_id AND lis.lexis_id = :lexis_id
        LIMIT 1;
    """)
    hits = (await session.execute(stmt, {'project_id': project_id, 'lexis_id': lexis_id})).mappings().one_or_none()
    if hits is None:
        return None
    return hits['item_id'], hits['item_source_id']


async def import_lexis_nexis(  # noqa: C901
    db_engine: DatabaseEngineAsync,
    project_id: str | uuid.UUID,
    new_items: LexisNexisItemGenerator,
    import_name: str | None = None,
    import_id: str | uuid.UUID | None = None,
    pipeline_task_id: str | None = None,
    user_id: str | uuid.UUID | None = None,
    description: str | None = None,
    vectoriser: CountVectorizer | None = None,
    max_slop: float = 0.05,
    min_text_len: int = 50,
    max_text_len: int = 800,
    batch_size: int = 2000,
    max_features: int = 5000,
    logger: logging.Logger | None = None,
    allow_empty_text: bool = False,
) -> tuple[str, int]:
    """
    Imports and deduplicates lexisnexis items.

    - Create revision
    - Check for ID duplicates (LexisNexisItemSource.lexis_id)
    - Create milvus index
    - Initialise milvus index with existing LexisNexisItem.text and new ones
    - for each new item
       - check for ID duplicate
       - check for duplicate in index
         -> attach source (and check first that LexisNexisItemSource.lexis_id is not there already)
       - else: create new item, lexis_item, and lexis_item_source and m2m


    :param max_text_len: For similarity check, cut texts longer than that; no need to compare more than that
    """

    if logger is None:
        logger = logging.getLogger('nacsos_data.util.lexisnexis.import')

    if project_id is None:
        raise AttributeError('You have to provide a project ID!')

    async with db_engine.session() as session:  # type: AsyncSession
        await set_session_mutex(session, project_id=project_id, lock=True)

        # Get the import and figure out what ids to deduplicate on, based on import type
        import_orm = await get_or_create_import(
            session=session,
            project_id=project_id,
            import_id=import_id,
            user_id=user_id,
            import_name=import_name,
            description=description,
            i_type='script',
        )
        import_id = str(import_orm.import_id)

        last_revision = await get_latest_revision(session=session, import_id=import_id)
        revision_counter = 1 if last_revision is None else last_revision.import_revision_counter + 1
        revision_id = str(uuid.uuid4())

        # Create a new revision
        session.add(
            ImportRevision(
                import_revision_id=revision_id,
                import_id=import_id,
                import_revision_counter=revision_counter,
                pipeline_task_id=pipeline_task_id,
            ),
        )
        await session.commit()  # Note, committing instead of flushing here, so this is persisted for reference even on error

        known_ids = await read_known_ids(session=session, project_id=project_id, logger=logger, allow_empty_text=allow_empty_text)
        logger.info(f'Loaded lookup with {len(known_ids):,} known LexisNexis source IDs')

    # Accumulator for our vocabulary
    token_counts: defaultdict[str, int] = defaultdict(int)
    n_unknown_items = 0

    with elapsed_timer(logger, f'Constructing vocabulary from {len(token_counts):,} `token_counts`'):
        for it, item in enumerate(new_items()):
            # Extend our vocabulary
            for tok in tokenise_item(item, lowercase=True):
                token_counts[tok] += 1
            n_unknown_items += 1

            if it > 20000:
                break

        vocabulary = extract_vocabulary(token_counts, min_count=1, max_features=max_features, skip_top=0.05)
        del token_counts  # clean up term counts to save RAM

    if vectoriser is None:
        with elapsed_timer(logger, f'Setting up vectorizer with {len(vocabulary):,} tokens in the vocabulary'):
            vectoriser = CountVectorizer(vocabulary=vocabulary)

    index: MilvusDuplicateIndex | None = None

    logger.debug('Constructing Milvus index...')
    async with db_engine.session() as session:  # type: AsyncSession
        with elapsed_timer(logger, '  -> preparing duplicate detection index...'):
            index = MilvusDuplicateIndex(
                existing_items=_item_entries_from_db(
                    session=session,
                    batch_size=batch_size,
                    project_id=project_id,
                    min_text_len=min_text_len,
                    max_text_len=max_text_len,
                    logger=logger,
                ),
                new_items=_filtered_entries(
                    new_items,
                    known_ids=known_ids,
                    min_text_len=min_text_len,
                    max_text_len=max_text_len,
                ),
                project_id=project_id,
                vectoriser=vectoriser,
                max_slop=max_slop,
                batch_size=batch_size,
            )

        with elapsed_timer(logger, '  -> initialising duplicate detection index...'):
            await index.init()

    logger.info('Finished pre-processing and index building.')
    logger.info('Proceeding to insert new items and creating m2m tuples...')
    num_updated = 0
    num_total = 0
    num_new = 0
    num_matched = 0
    async with db_engine.session() as session:  # type: AsyncSession
        for new_item in new_items():
            num_total += 1
            try:
                async with session.begin_nested():
                    with elapsed_timer(logger, f'Importing LexisNexis item with ID {new_item.sources[0].lexis_id} and title "{new_item.sources[0].title}"'):  # type: ignore[index]
                        # Check if we've seen this lexis item before based on source ID
                        for source in new_item.sources or []:
                            # Quick check in the lookup index from earlier
                            if source.lexis_id in known_ids:
                                await upsert_m2m(
                                    session=session,
                                    item_id=known_ids[source.lexis_id],
                                    import_id=import_id,
                                    latest_revision=revision_counter,
                                    logger=logger,
                                    dry_run=False,
                                )
                                num_matched += 1
                                break

                            # Expensive check to make sure we didn't add it in the meantime
                            new_known = await find_by_source(session=session, project_id=project_id, lexis_id=source.lexis_id)
                            if new_known is not None:
                                await upsert_m2m(
                                    session=session,
                                    item_id=str(new_known[0]),
                                    import_id=import_id,
                                    latest_revision=revision_counter,
                                    logger=logger,
                                    dry_run=False,
                                )
                                num_matched += 1
                                break

                        # We have not seen this lexis item based on the source ID before
                        else:
                            item_id: uuid.UUID | None = None

                            # When we have enough text, check our similarity index
                            if item.text is not None and len(item.text) > min_text_len:
                                item_id = index.test(ItemEntry(item_id=str(item.item_id), text=item.text[:max_text_len]))  # type: ignore[assignment]

                            # We have not found anything, add new LexisNexisItem!
                            if item_id is None:
                                item_id = uuid.uuid4()
                                new_item.item_id = item_id
                                new_item.project_id = project_id
                                session.add(LexisNexisItem(**item.model_dump(exclude={'sources'})))
                                await session.flush()
                                num_new += 1
                            else:
                                num_updated += 1

                            # Add all the sources (no need to double-check source existence again)
                            # This either adds the source to the exising item we found or the newly created item
                            for source in item.sources or []:
                                if source.item_source_id is None:
                                    source.item_source_id = str(uuid.uuid4())
                                source.item_id = item.item_id
                                session.add(LexisNexisItemSource(**source.model_dump()))

                            # Link item to import revision
                            await upsert_m2m(
                                session,
                                item_id=str(item.item_id),
                                logger=logger,
                                import_id=import_id,
                                dry_run=False,
                                latest_revision=revision_counter,
                            )

                        await session.flush()

            except (UniqueViolation, IntegrityError, OperationalError) as e:
                logger.exception(e)

            logger.info(f'Processed {num_total} items, matched {num_matched}, updated {num_updated}, and added {num_new} items.')

        # Commit all changes
        await session.commit()

    # Open new session for cleanup; just in case we got hung up earlier
    async with db_engine.session() as session:  # type: AsyncSession
        logger.info('Updating revision stats')
        await update_revision_statistics(
            session=session,
            import_id=import_id,
            revision_id=revision_id,
            latest_revision=revision_counter,
            num_new_items=num_new,
            num_updated=num_updated,
            logger=logger,
        )

        # Free our import mutex
        await set_session_mutex(session, project_id=project_id, lock=False)

    with elapsed_timer(logger, 'Cleaning up milvus!'):
        index.client.drop_collection(index.collection_name)

    # Return to caller
    return import_id, revision_counter
