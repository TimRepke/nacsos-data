import logging
from sklearn.feature_extraction.text import CountVectorizer
import pynndescent

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=logging.INFO)
logger = logging.getLogger('deduplicate')
logger.setLevel(logging.DEBUG)

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
# Number of candidates to fetch
N_CANDIDATES = 5
# Maximum proportion of dissimilar words
MAX_SLOP = 0.02

# FIXME: This is untested and should be considered work in progress
# TODO: use .index for indexing
# TODO: do full cross-join incl self deduplication
# TODO: go beyond text matching; also use other checks


# Datasets to match
# Assuming you have some sort of index (first entry in each tuple) and text (second entry in tuple)
def left_join(data_a: list[tuple[str, str]],
              data_b: list[tuple[str, str]]):
    logger.info('Preparing texts...')
    _texts_a = [(t or '').lower()[:CHAR_LIMIT] for _, t in data_a]
    _texts_b = [(t or '').lower()[:CHAR_LIMIT] for _, t in data_b]

    logger.info('Filtering data texts...')
    texts_a = [t for t in _texts_a if len(t) >= MIN_TEXT_LEN]
    texts_b = [t for t in _texts_b if len(t) >= MIN_TEXT_LEN]

    logger.info('Filtering data ids...')
    ids_a = [i for (i, _), t in zip(data_a, _texts_a) if len(t) >= MIN_TEXT_LEN]
    ids_b = [i for (i, _), t in zip(data_b, _texts_b) if len(t) >= MIN_TEXT_LEN]
    ignored_ids_a = [i for (i, _), t in zip(data_a, _texts_a) if len(t) < MIN_TEXT_LEN]
    ignored_ids_b = [i for (i, _), t in zip(data_b, _texts_b) if len(t) < MIN_TEXT_LEN]

    logger.info('Building ID lookup maps...')
    id_lookup_a = {idx: i for idx, i in enumerate(ids_a)}
    id_lookup_b = {idx: i for idx, i in enumerate(ids_b)}
    id_lookup_a_inv = {i: idx for idx, i in id_lookup_a.items()}
    id_lookup_b_inv = {i: idx for idx, i in id_lookup_b.items()}

    logger.info('Fitting vocabulary...')
    vectoriser = CountVectorizer(min_df=MIN_DF, max_df=MAX_DF, max_features=MAX_FEATURES)
    vectoriser.fit(texts_a + texts_b)

    logger.info('Creating vectors from texts...')
    vectors_a = vectoriser.transform(texts_a)
    vectors_b = vectoriser.transform(texts_b)

    logger.info('Constructing nearest neighbour lookup...')
    # Using Jaccard dissimilarity, defined as: 1 - (token set intersection divided by token set union)
    index = pynndescent.NNDescent(vectors_a, metric='jaccard')

    logger.info('Querying nearest neighbour lookup...')
    indices, similarities = index.query(vectors_b, k=N_CANDIDATES)

    logger.info('Proceeding with post-processing...')
    matches = {}
    for i, near_idxs, near_similarities in zip(ids_b, indices, similarities):
        logger.debug(f'Checking match for {i}: {near_similarities}')
        for idx, similarity in zip(near_idxs, near_similarities):
            # Too dissimilar, we can stop right here (note: list is sorted asc)
            if similarity > MAX_SLOP:
                logger.debug(f' -> No close text match with >{1 - MAX_SLOP} overlap')
                break
            if i not in matches:
                matches[i] = []
            matches[i].append(id_lookup_a_inv[idx])

    return matches, ignored_ids_a, ignored_ids_b
