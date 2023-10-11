import logging
import uuid
from datetime import datetime
from typing import Generator, AsyncGenerator

from scipy.sparse import vstack, csr_matrix
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.base import TransformerMixin

import pynndescent

from sqlalchemy import select, insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
import sqlalchemy.sql.functions as F

from nacsos_data.db.engine import ensure_session_async
from nacsos_data.db.schemas import LexisNexisItem, LexisNexisItemSource, Item, m2m_import_item_table

from .parse import parse_lexis_nexis_file
from .. import batched
from ...db.crud.imports import get_or_create_import
from ...models.imports import M2MImportItemType

logger = logging.getLogger('nacsos_data.util.LexisNexis')

# Texts shorter than N characters will always be assumed unique (excluded from deduplication)
MIN_TEXT_LEN = 10
# Min. number of documents (parameter for CountVectorizer)
MIN_DF = 5
# Max. proportion of documents (parameter for CountVectorizer)
MAX_DF = 0.98
# Max. number of features / vocabulary size (parameter for CountVectorizer)
MAX_FEATURES = 2000


def has_text(s: str | None) -> bool:
    return s is not None and len(s) > MIN_TEXT_LEN


async def load_vectors_from_database(session: AsyncSession,
                                     project_id: str,
                                     vectoriser: CountVectorizer,
                                     batch_size: int = 500) \
        -> AsyncGenerator[tuple[list[str], csr_matrix], None]:
    stmt = (select(Item.text, Item.item_id)
            .where(Item.project_id == project_id,
                   Item.text.isnot(None),
                   F.char_length(Item.text) > MIN_TEXT_LEN)
            .execution_options(yield_per=batch_size))
    rslt = (await session.execute(stmt)).mappings().partitions()

    for batch in rslt:
        logger.debug(f'Received batch with {len(batch)} entries.')
        item_ids = [str(r['item_id']) for r in batch]
        texts = [r['text'].lower() for r in batch]

        if not vectoriser.vocabulary_:
            logger.debug('Vectoriser has no vocabulary, fitting it now on the first batch.')
            vectoriser.fit(texts)

        vectors = vectoriser.transform(texts)

        yield item_ids, vectors


def load_vectors_from_file(filename: str,
                           vectoriser: CountVectorizer,
                           fail_on_error: bool = True,
                           batch_size: int = 10000) -> Generator[csr_matrix, None, None]:
    for batch in batched(parse_lexis_nexis_file(filename=filename, fail_on_error=fail_on_error),
                         batch_size=batch_size):
        batch_filtered = [(r, s) for _, r, s in batch if r.text is not None and len(r.text) > MIN_TEXT_LEN]
        if len(batch_filtered) == 0:
            continue

        lexis_ids = [s.lexis_id for _, s in batch_filtered]
        item_ids = [r.item_id for r, _ in batch_filtered]
        texts = [r.text.lower() for r, _ in batch_filtered]

        if not hasattr(vectoriser, 'vocabulary_'):
            vectoriser.fit(texts)

        vectors = vectoriser.transform(texts)

        yield lexis_ids, item_ids, vectors


@ensure_session_async
async def import_lexis_nexis(session: AsyncSession,
                             project_id: str,
                             filename: str,
                             import_name: str | None = None,
                             import_id: str | uuid.UUID | None = None,
                             user_id: str | uuid.UUID | None = None,
                             description: str | None = None,
                             vectoriser: TransformerMixin | None = None,
                             batch_size_db: int = 500,
                             batch_size_file: int = 10000,
                             max_slop: float = 0.02,
                             dedup_source: bool = True,
                             fail_on_parse_error: bool = True,
                             log: logging.Logger | None = None) -> None:
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

    # Keep track of when we started importing
    import_orm.time_started = datetime.now()

    if vectoriser is None:
        vectoriser = CountVectorizer(min_df=MIN_DF, max_df=MAX_DF, max_features=MAX_FEATURES)

    log.info(f'Loading known ids from project {project_id}')
    stmt = (select(LexisNexisItemSource.item_id, LexisNexisItemSource.lexis_id)
            .where(LexisNexisItemSource.article.project_id == project_id))
    known_ids = (await session.execute(stmt)).mappings().all()

    ln2id = {row['lexis_id']: str(row['item_id']) for row in known_ids}

    log.info(f'Loading texts from project {project_id}')
    db_data = [r async for r in load_vectors_from_database(session=session, project_id=project_id,
                                                           batch_size=batch_size_db, vectoriser=vectoriser)]
    item_ids_db = {r: i for i, r in enumerate(bid for batch_ids, _ in db_data for bid in batch_ids)}
    item_ids_db_inv = {v: k for k, v in item_ids_db.items()}

    offset = len(item_ids_db)

    log.info(f'Loading texts (and vectors) from file: {filename}')
    f_data = list(load_vectors_from_file(filename=filename, fail_on_error=fail_on_parse_error,
                                         batch_size=batch_size_file, vectoriser=vectoriser))
    # Note: We cannot trust that lexis_ids are unique here, but we assume it's fine to any of the
    #       documents with the same ID, so by default we pick the last seen for simplicity.
    lexis_ids_f_inv = {i + offset: r for i, r in enumerate(bid for batch_ids, _, _ in f_data for bid in batch_ids)}
    item_ids_f = {r: i + offset for i, r in enumerate(bid for _, batch_ids, _ in f_data for bid in batch_ids)}
    item_ids_f_inv = {v: k for k, v in item_ids_f.items()}

    vectors = vstack([batch_vectors for _, batch_vectors in db_data]
                     + [batch_vectors for _, _, batch_vectors in f_data])

    # Using Jaccard dissimilarity, defined as: 1 - (token set intersection divided by token set union)
    index = pynndescent.NNDescent(vectors, metric='jaccard')

    for result, item, source in parse_lexis_nexis_file(filename=filename, fail_on_error=fail_on_parse_error):
        existing_id: str | None = None

        if source.lexis_id is None:
            raise ValueError('Document is missing a LexisNexis ID')

        # Naive deduplication: We've already seen the same LexisNexis ID
        if source.lexis_id in ln2id:
            existing_id = ln2id[source.lexis_id]

        if item.text is not None and len(item.text) > MIN_TEXT_LEN:
            vector = vectoriser.transform([item.text])
            indices, similarities = index.query(vector, k=5)
            for index, similarity in zip(indices, similarities):
                # Too dissimilar, we can stop right here (note: list is sorted asc)
                if similarity > max_slop:
                    break

                # Looking at itself, continue
                if (item_ids_db_inv.get(index) == item.item_id) or (item_ids_f_inv.get(index) == item.item_id):
                    continue

                # See, if we already stored this in the database ahead of time
                if index in item_ids_db_inv:
                    existing_id = item_ids_db_inv[index]
                    break
                # See, if we've seen this and saved this already in the process
                elif index in lexis_ids_f_inv and lexis_ids_f_inv[index] in ln2id:
                    existing_id = ln2id[lexis_ids_f_inv[index]]
                    break
                # else: false positive, it's a duplicate and we just saw the first one of them

        # This seems to be a novel item we haven't seen before
        if existing_id is None:
            new_id = str(item.item_id)
            ln2id[source.lexis_id] = new_id
            session.add(LexisNexisItem(**item.model_dump()))
            await session.commit()
            session.add(LexisNexisItemSource(**source.model_dump()))
            await session.commit()

            stmt_m2m = (insert(m2m_import_item_table)
                        .values(item_id=new_id, import_id=import_id, type=M2MImportItemType.explicit))
            try:
                await session.execute(stmt_m2m)
                await session.commit()
                log.debug(' -> Added many-to-many relationship for import/item')
            except IntegrityError:
                log.debug(f' -> M2M_i2i already exists, ignoring {import_id} <-> {new_id}')
                await session.rollback()

        # This seems to be a duplicate of a known item
        else:
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
                session.add(LexisNexisItemSource(**source.model_dump()))
                await session.commit()

        # Keep track of when we finished importing
        import_orm.time_finished = datetime.now()
        await session.commit()
