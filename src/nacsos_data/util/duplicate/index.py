import logging
from typing import TYPE_CHECKING, Generator, AsyncGenerator, NamedTuple
from scipy.sparse import vstack, csr_matrix
from sklearn.feature_extraction.text import CountVectorizer

import pynndescent

from .. import batched

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401

logger = logging.getLogger('nacsos_data.util.deduplicate.index')


class ItemEntry(NamedTuple):
    item_id: str
    text: str


class DuplicateIndex:
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
    # Number of candidates to look at during query time
    N_CANDIDATES = 5

    def __init__(self,
                 existing_items: AsyncGenerator[list[ItemEntry], None],
                 new_items: Generator[ItemEntry, None, None],
                 vectoriser: CountVectorizer | None = None,
                 max_slop: float = 0.02,
                 batch_size: int = 10000):

        self.existing_items = existing_items
        self.new_items = new_items
        self.max_slop = max_slop
        self.batch_size = batch_size
        if vectoriser is None:
            self.vectoriser = CountVectorizer(min_df=self.MIN_DF, max_df=self.MAX_DF, max_features=self.MAX_FEATURES)
        else:
            self.vectoriser = vectoriser

        self.index: pynndescent.NNDescent | None = None
        self.item_ids_db: dict[str, int] | None = None
        self.item_ids_db_inv: dict[int, str] | None = None
        self.item_ids_nw: dict[str, int] | None = None
        self.item_ids_nw_inv: dict[int, str] | None = None

        self.saved: dict[str, str] = {}

    async def _load_vectors_batched_async(self, generator: AsyncGenerator[list[ItemEntry], None]) \
            -> AsyncGenerator[tuple[list[str], csr_matrix], None]:
        async for batch in generator:
            logger.debug(f'Received batch with {len(batch)} entries.')
            item_ids = [r.item_id for r in batch]
            texts = [r.text.lower() for r in batch]

            if not hasattr(self.vectoriser, 'vocabulary_') and len(texts) > self.MIN_DF:
                logger.debug('Vectoriser has no vocabulary, fitting it now on the first batch.')
                self.vectoriser.fit(texts)

            vectors = self.vectoriser.transform(texts)

            yield item_ids, vectors

    def _load_vectors_sync(self, generator: Generator[ItemEntry, None, None]) \
            -> Generator[tuple[list[str], csr_matrix], None, None]:
        for batch in batched(generator, batch_size=self.batch_size):
            logger.debug(f'Received batch with {len(batch)} entries.')
            item_ids = [r.item_id for r in batch]
            texts = [r.text.lower() for r in batch]

            if not hasattr(self.vectoriser, 'vocabulary_') and len(texts) > self.MIN_DF:
                logger.debug('Vectoriser has no vocabulary, fitting it now on the first batch.')
                self.vectoriser.fit(texts)

            vectors = self.vectoriser.transform(texts)

            yield item_ids, vectors

    async def init(self) -> None:
        logger.info('Loading items from database...')
        db_data = [r async for r in self._load_vectors_batched_async(self.existing_items)]

        self.item_ids_db = {r: i for i, r in enumerate(bid for batch_ids, _ in db_data for bid in batch_ids)}
        self.item_ids_db_inv = {v: k for k, v in self.item_ids_db.items()}
        offset = len(self.item_ids_db)
        logger.info(f'Found {offset:,} documents already in the database.')

        logger.info('Loading items from new source...')
        nw_data = list(self._load_vectors_sync(self.new_items))

        self.item_ids_nw = {r: i + offset for i, r in
                            enumerate(bid for batch_ids, _ in nw_data for bid in batch_ids)}
        self.item_ids_nw_inv = {v: k for k, v in self.item_ids_nw.items()}
        logger.info(f'Found {len(self.item_ids_nw):,} documents in the file.')

        logger.info('Constructing nearest neighbour lookup...')
        vectors = vstack([batch_vectors for _, batch_vectors in db_data]
                         + [batch_vectors for _, batch_vectors in nw_data])
        # Using Jaccard dissimilarity, defined as: 1 - (token set intersection divided by token set union)
        self.index = pynndescent.NNDescent(vectors, metric='jaccard')

    def test(self, item: ItemEntry) -> str | None:
        if self.index is None:
            raise RuntimeError('Index is not initialised, yet!')
        if self.item_ids_db_inv is None or self.item_ids_nw_inv is None:
            raise RuntimeError('Lookups are not initialised, yet!')

        if item.text is not None and len(item.text) > self.MIN_TEXT_LEN:
            vector = self.vectoriser.transform([item.text])
            indices, similarities = self.index.query(vector, k=self.N_CANDIDATES)

            for vec_index, similarity in zip(indices[0], similarities[0]):
                # Too dissimilar, we can stop right here (note: list is sorted asc)
                if similarity > self.max_slop:
                    logger.debug(f' -> No close text match with >{1 - self.max_slop} overlap')
                    return None

                item_id_db = self.item_ids_db_inv.get(vec_index)
                item_id_nw = self.item_ids_nw_inv.get(vec_index)

                # Looking at itself, continue
                if (item_id_db == item.item_id) or (item_id_nw == item.item_id):
                    continue

                # See, if we already stored this in the database ahead of time
                if item_id_db is not None:
                    logger.debug(' -> Found text match in database')
                    return item_id_db

                # See, if we've seen this and saved this already in the process
                if item_id_nw is not None and item_id_nw in self.saved:
                    logger.debug(' -> Found text match in new items (but we sent it to the database earlier)')
                    return self.saved[item_id_nw]

                # else: false positive, it's a duplicate and we just saw the first one of them
        return None

    # Follow chains of duplicate reference if needed to resolve to the "origin" duplicate
    def _resolve(self, cid: str) -> str:
        if cid in self.saved:
            return self._resolve(self.saved[cid])
        return cid

    def register_stored(self, new_id: str, existing_id: str | None) -> None:
        if existing_id is None:
            self.saved[new_id] = new_id
        else:
            self.saved[new_id] = self._resolve(existing_id)
