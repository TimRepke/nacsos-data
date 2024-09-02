import logging
import uuid
from typing import Generator, AsyncGenerator, TYPE_CHECKING

from sqlalchemy import select, insert, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
import sqlalchemy.sql.functions as F

from nacsos_data.db.engine import ensure_session_async, DBSession
from nacsos_data.db.schemas import LexisNexisItem, LexisNexisItemSource, Item, m2m_import_item_table

from .parse import parse_lexis_nexis_file
from .. import batched
from ...db.crud.imports import get_or_create_import
from ...models.imports import M2MImportItemType

if TYPE_CHECKING:
    from scipy.sparse import csr_matrix
    from sklearn.feature_extraction.text import CountVectorizer

logger = logging.getLogger('nacsos_data.util.LexisNexis')

# Texts shorter than N characters will always be assumed unique (excluded from deduplication)
MIN_TEXT_LEN = 10
# Min. number of documents (parameter for CountVectorizer)
MIN_DF = 5
# Max. proportion of documents (parameter for CountVectorizer)
MAX_DF = 0.98
# Max. number of features / vocabulary size (parameter for CountVectorizer)
MAX_FEATURES = 2000
# Cutting texts longer than that, no need to compare more than that
CHAR_LIMIT = 800


def has_text(s: str | None) -> bool:
    return s is not None and len(s) > MIN_TEXT_LEN


async def load_vectors_from_database(session: AsyncSession,
                                     project_id: str,
                                     vectoriser: CountVectorizer,
                                     batch_size: int = 500) \
        -> AsyncGenerator[tuple[list[str], csr_matrix], None]:
    stmt = (select(func.substr(Item.text, 0, CHAR_LIMIT), Item.item_id)
            .where(Item.project_id == project_id,
                   Item.text.isnot(None),
                   F.char_length(Item.text) > MIN_TEXT_LEN)
            .execution_options(yield_per=batch_size))
    rslt = (await session.stream(stmt)).mappings().partitions()

    async for batch in rslt:
        logger.debug(f'Received batch with {len(batch)} entries.')
        item_ids = [str(r['item_id']) for r in batch]
        texts = [r['text'].lower() for r in batch]

        if not hasattr(vectoriser, 'vocabulary_') and len(texts) > MIN_DF:
            logger.debug('Vectoriser has no vocabulary, fitting it now on the first batch.')
            vectoriser.fit(texts)

        vectors = vectoriser.transform(texts)

        yield item_ids, vectors


def load_vectors_from_file(filename: str,
                           vectoriser: CountVectorizer,
                           fail_on_error: bool = True,
                           batch_size: int = 10000) -> Generator[tuple[list[str], list[str], csr_matrix], None, None]:
    for batch in batched(parse_lexis_nexis_file(filename=filename, fail_on_error=fail_on_error),
                         batch_size=batch_size):
        batch_filtered = [(r, s) for _, r, s in batch if r.text is not None and len(r.text) > MIN_TEXT_LEN]

        # Nothing to see here, please carry on
        if len(batch_filtered) == 0:
            continue

        lexis_ids = [s.lexis_id for _, s in batch_filtered]
        item_ids = [str(r.item_id) for r, _ in batch_filtered]
        texts = [(r.text or '').lower()[:CHAR_LIMIT] for r, _ in batch_filtered]

        if not hasattr(vectoriser, 'vocabulary_'):
            vectoriser.fit(texts)

        vectors = vectoriser.transform(texts)

        yield lexis_ids, item_ids, vectors


@ensure_session_async
async def import_lexis_nexis(session: DBSession,
                             project_id: str,
                             filename: str,
                             import_name: str | None = None,
                             import_id: str | uuid.UUID | None = None,
                             user_id: str | uuid.UUID | None = None,
                             description: str | None = None,
                             vectoriser: CountVectorizer | None = None,
                             batch_size_db: int = 500,
                             batch_size_file: int = 10000,
                             max_slop: float = 0.02,
                             dedup_source: bool = True,
                             fail_on_parse_error: bool = True,
                             log: logging.Logger | None = None) -> None:
    """
    Imports and deduplicates lexisnexis items.

    - Create import
    - It first gets all existing data from the database in that project (project_id)
    - Then reads the full source file (filename)
    - While doing that, transforms texts into vectors; vocab constructed from first seen batch
    - Create lookup index (approx. nearest neighbours)
    - Read source file line by line
      - check if lexis_id is already known
      - check if text exists in index (jaccard < max_slop)
      - add item source (where article was published)
      - add item if not exists
      - create m2m relation

    Example usage:

    ```
    import asyncio
    import logging

    from sqlalchemy.ext.asyncio import AsyncSession

    from nacsos_data.util.lexisnexis.importer import import_lexis_nexis
    from nacsos_data.db import get_engine_async

    PROJECT_ID = '???'
    USER_ID = '???'

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=logging.INFO)
    logger = logging.getLogger('LN IMPORT')
    logger.setLevel(logging.DEBUG)


    async def main():
        db_engine = get_engine_async(conf_file='/path/to/config.env')
        async with db_engine.session() as session:  # type: AsyncSession
            await import_lexis_nexis(session=session,
                                     project_id=PROJECT_ID,
                                     filename='lexis_sample.jsonl',
                                     import_name='LN import',
                                     user_id=USER_ID,
                                     description='test import',
                                     log=logger)


    if __name__ == '__main__':
        asyncio.run(main())
    ```

    :param session:
    :param project_id:
    :param filename:
    :param import_name:
    :param import_id:
    :param user_id:
    :param description:
    :param vectoriser:
    :param batch_size_db:
    :param batch_size_file:
    :param max_slop:
    :param dedup_source:
    :param fail_on_parse_error:
    :param log:
    :return:
    """
    import pynndescent
    from scipy.sparse import vstack

    if log is None:
        log = logger

    import_orm = await get_or_create_import(session=session,
                                            project_id=project_id,
                                            import_id=import_id,
                                            user_id=user_id,
                                            import_name=import_name,
                                            description=description,
                                            i_type='script')
    import_id = str(import_orm.import_id)

    if vectoriser is None:
        from sklearn.feature_extraction.text import CountVectorizer
        vectoriser = CountVectorizer(min_df=MIN_DF, max_df=MAX_DF, max_features=MAX_FEATURES)

    log.info(f'Loading known ids from project {project_id}')
    stmt = (select(LexisNexisItemSource.item_id, LexisNexisItemSource.lexis_id)
            .join(LexisNexisItem, LexisNexisItem.item_id == LexisNexisItemSource.item_id)
            .where(LexisNexisItem.project_id == project_id))

    known_ids = (await session.execute(stmt)).mappings().all()

    ln2id = {row['lexis_id']: str(row['item_id']) for row in known_ids}

    if len(ln2id) > 0:
        log.info(f'Loading texts from project {project_id}')
        db_data = [r async for r in load_vectors_from_database(session=session, project_id=project_id,
                                                               batch_size=batch_size_db, vectoriser=vectoriser)]
        item_ids_db = {r: i for i, r in enumerate(bid for batch_ids, _ in db_data for bid in batch_ids)}
        item_ids_db_inv = {v: k for k, v in item_ids_db.items()}
    else:
        db_data = []
        item_ids_db = {}
        item_ids_db_inv = {}

    offset = len(item_ids_db)
    log.info(f'Found {offset:,} documents already in the database.')

    log.info(f'Loading texts (and vectors) from file: {filename}')
    f_data = list(load_vectors_from_file(filename=filename, fail_on_error=fail_on_parse_error,
                                         batch_size=batch_size_file, vectoriser=vectoriser))
    # Note: We cannot trust that lexis_ids are unique here, but we assume it's fine to any of the
    #       documents with the same ID, so by default we pick the last seen for simplicity.
    lexis_ids_f_inv = {i + offset: r for i, r in enumerate(bid for batch_ids, _, _ in f_data for bid in batch_ids)}
    item_ids_f = {r: i + offset for i, r in enumerate(bid for _, batch_ids, _ in f_data for bid in batch_ids)}
    item_ids_f_inv = {v: k for k, v in item_ids_f.items()}

    log.info(f'Found {len(item_ids_f):,} documents in the file.')

    vectors = vstack([batch_vectors for _, batch_vectors in db_data]
                     + [batch_vectors for _, _, batch_vectors in f_data])

    log.info('Constructing nearest neighbour lookup...')
    # Using Jaccard dissimilarity, defined as: 1 - (token set intersection divided by token set union)
    index = pynndescent.NNDescent(vectors, metric='jaccard')

    log.info('Going through the file now and importing articles...')
    for result, item, source in parse_lexis_nexis_file(filename=filename,
                                                       project_id=project_id,
                                                       fail_on_error=fail_on_parse_error):
        try:
            async with session.begin_nested():
                log.debug(f'Importing "{source.title}" ({source.lexis_id})')
                existing_id: str | None = None

                if source.lexis_id is None:
                    raise ValueError(' -> Document is missing a LexisNexis ID')

                # Naive deduplication: We've already seen the same LexisNexis ID
                if source.lexis_id in ln2id:
                    existing_id = ln2id[source.lexis_id]
                    log.debug(f' -> Duplicate by LN id: {source.lexis_id}')

                if item.text is not None and len(item.text) > MIN_TEXT_LEN:
                    vector = vectoriser.transform([item.text])
                    indices, similarities = index.query(vector, k=5)
                    for vec_index, similarity in zip(indices[0], similarities[0]):
                        # Too dissimilar, we can stop right here (note: list is sorted asc)
                        if similarity > max_slop:
                            log.debug(f' -> No close text match with >{1 - max_slop} overlap')
                            break

                        # Looking at itself, continue
                        if (item_ids_db_inv.get(vec_index) == item.item_id) or (item_ids_f_inv.get(vec_index) == item.item_id):
                            continue

                        # See, if we already stored this in the database ahead of time
                        if vec_index in item_ids_db_inv:
                            existing_id = item_ids_db_inv[vec_index]
                            log.debug(' -> Found text match in database')
                            break
                        # See, if we've seen this and saved this already in the process
                        elif vec_index in lexis_ids_f_inv and lexis_ids_f_inv[vec_index] in ln2id:
                            existing_id = ln2id[lexis_ids_f_inv[vec_index]]
                            log.debug(' -> Found text match in file (but we sent it to the database earlier)')
                            break
                        # else: false positive, it's a duplicate and we just saw the first one of them

                # This seems to be a novel item we haven't seen before
                if existing_id is None:
                    new_id = str(item.item_id)
                    log.debug(f' -> Creating new item with ID={new_id}')
                    ln2id[source.lexis_id] = new_id
                    session.add(LexisNexisItem(**item.model_dump()))
                    await session.flush()
                    session.add(LexisNexisItemSource(**source.model_dump()))
                    await session.flush()

                    stmt_m2m = (insert(m2m_import_item_table)
                                .values(item_id=new_id, import_id=import_id, type=M2MImportItemType.explicit))
                    await session.execute(stmt_m2m)
                    await session.flush()
                    log.debug(' -> Added many-to-many relationship for import/item')

                # This seems to be a duplicate of a known item
                else:
                    log.debug(f' -> Trying to add source to existing item with item_id={existing_id}')
                    source.item_id = existing_id
                    source_unique = True
                    if dedup_source:
                        stmt = (select(F.count(LexisNexisItemSource.item_source_id))
                                .where(LexisNexisItemSource.item_id == existing_id,
                                       LexisNexisItemSource.lexis_id == source.lexis_id))
                        n_results = await session.scalar(stmt)
                        if n_results is not None and n_results > 0:
                            source_unique = False

                    if source_unique:
                        log.debug(f' -> Adding source to existing item with source_id={source.item_source_id}')
                        session.add(LexisNexisItemSource(**source.model_dump()))
                        await session.flush()

        except IntegrityError:
            log.debug(f' -> M2M_i2i already exists, ignoring {import_id} <-> {new_id}')

    await session.flush_or_commit()
    log.info('All done!')
