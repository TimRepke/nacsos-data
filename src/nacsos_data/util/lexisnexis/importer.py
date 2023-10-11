import logging
from typing import Generator, AsyncGenerator

from scipy.sparse import vstack, csr_matrix
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.base import TransformerMixin

import pynndescent

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import sqlalchemy.sql.functions as F

from nacsos_data.db.engine import ensure_session_async
from nacsos_data.db.schemas import LexisNexisItem, LexisNexisItemSource, Item

from .parse import parse_lexis_nexis_file
from .. import batched

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
        -> AsyncGenerator[tuple[list[str], list[str], csr_matrix], None]:
    stmt = (select(Item.text, Item.item_id, LexisNexisItem.lexis_id)
            .where(Item.project_id == project_id,
                   Item.text.isnot(None),
                   F.char_length(Item.text) > MIN_TEXT_LEN)
            .execution_options(yield_per=batch_size))
    rslt = (await session.execute(stmt)).mappings().partitions()

    for batch in rslt:
        logger.debug(f'Received batch with {len(batch)} entries.')
        lexis_ids = [r['lexis_id'] for r in batch]
        item_ids = [str(r['item_id']) for r in batch]
        texts = [r['text'].lower() for r in batch]

        if not vectoriser.vocabulary_:
            logger.debug('Vectoriser has no vocabulary, fitting it now on the first batch.')
            vectoriser.fit(texts)

        vectors = vectoriser.transform(texts)

        yield lexis_ids, item_ids, vectors


def load_vectors_from_file(filename: str,
                           vectoriser: CountVectorizer,
                           fail_on_error: bool = True,
                           batch_size: int = 10000) -> Generator[csr_matrix, None, None]:
    for batch in batched(parse_lexis_nexis_file(filename=filename, fail_on_error=fail_on_error),
                         batch_size=batch_size):
        batch_filtered = [r for _, r, _ in batch if r.text is not None and len(r.text) > MIN_TEXT_LEN]
        if len(batch_filtered) == 0:
            continue

        lexis_ids = [r.lexis_id for r in batch_filtered]
        item_ids = [r.item_id for r in batch_filtered]
        texts = [r.text.lower() for r in batch_filtered]

        if not hasattr(vectoriser, 'vocabulary_'):
            vectoriser.fit(texts)

        vectors = vectoriser.transform(texts)

        yield lexis_ids, item_ids, vectors


@ensure_session_async
async def construct_lookup(session: AsyncSession,
                           project_id: str, filename: str,
                           vectoriser: TransformerMixin | None = None,
                           batch_size_db: int = 500,
                           batch_size_file: int = 10000,
                           max_slop: float = 0.02,
                           dedup_source: bool = True,
                           fail_on_parse_error: bool = True) -> None:
    if vectoriser is None:
        vectoriser = CountVectorizer(min_df=MIN_DF, max_df=MAX_DF, max_features=MAX_FEATURES)

    logger.info(f'Loading texts from project {project_id}')
    db_data = [r async for r in load_vectors_from_database(session=session, project_id=project_id,
                                                           batch_size=batch_size_db, vectoriser=vectoriser)]
    item_ids_db = {r: i for i, r in enumerate(bid for _, batch_ids, _ in db_data for bid in batch_ids)}
    item_ids_db_inv = {v: k for k, v in item_ids_db.items()}

    offset = len(item_ids_db)

    f_data = list(load_vectors_from_file(filename=filename, fail_on_error=fail_on_parse_error,
                                         batch_size=batch_size_file, vectoriser=vectoriser))
    # Note: We cannot trust that lexis_ids are unique here, but we assume it's fine to any of the
    #       documents with the same ID, so by default we pick the last seen for simplicity.
    lexis_ids_f_inv = {i + offset: r for i, r in enumerate(bid for batch_ids, _, _ in f_data for bid in batch_ids)}
    item_ids_f = {r: i + offset for i, r in enumerate(bid for _, batch_ids, _ in f_data for bid in batch_ids)}
    item_ids_f_inv = {v: k for k, v in item_ids_f.items()}

    vectors = vstack([batch_vectors for _, _, batch_vectors in db_data]
                     + [batch_vectors for _, _, batch_vectors in f_data])

    # Using Jaccard dissimilarity, defined as: 1 - (token set intersection divided by token set union)
    index = pynndescent.NNDescent(vectors, metric='jaccard')

    ln2id = {lnid: iid for iids, lnids, _ in db_data for iid, lnid in zip(iids, lnids)}

    for result, item, source in parse_lexis_nexis_file(filename=filename, fail_on_error=fail_on_parse_error):
        existing_id: str | None = None

        if item.lexis_id is None:
            raise ValueError('Document is missing a LexisNexis ID')

        # Naive deduplication: We've already seen the same LexisNexis ID
        if item.lexis_id in ln2id:
            existing_id = ln2id[item.lexis_id]

        if item.text is not None and len(item.text) > MIN_TEXT_LEN:
            vector = vectoriser.transform([item.text])
            indices, similarities = index.query(vector, k=5)
            for index, similarity in zip(indices, similarities):
                # Looking at itself, continue
                if (item_ids_db_inv.get(index) == item.item_id) or (item_ids_f_inv.get(index) == item.item_id):
                    continue

                if similarity < max_slop:
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
            ln2id[item.lexis_id] = str(item.item_id)
            session.add(LexisNexisItem(**item.model_dump()))
            await session.commit()
            session.add(LexisNexisItemSource(**source.model_dump()))
            await session.commit()

        # This seems to be a duplicate of a known item
        else:
            source.item_id = existing_id
            source_unique = True
            if dedup_source:
                stmt = (select(F.count(LexisNexisItemSource.item_source_id))
                        .where(LexisNexisItemSource.item_id == existing_id,
                               LexisNexisItemSource.published_at == source.published_at,
                               LexisNexisItemSource.updated_at == source.updated_at,
                               LexisNexisItemSource.name == source.name))
                n_results = await session.scalar(stmt)
                if n_results is not None and n_results > 0:
                    source_unique = False

            if source_unique:
                session.add(LexisNexisItemSource(**source.model_dump()))
                await session.commit()
