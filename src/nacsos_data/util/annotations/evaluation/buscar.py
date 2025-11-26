from typing import Iterator

from scipy.stats import hypergeom, nchypergeom_wallenius
import numpy as np
import numpy.typing as npt

from nacsos_data.models.annotation_tracker import H0Series

Array = np.ndarray[tuple[int], np.dtype[np.int64]]
ArrayOrList = Array | list[int]
ArrayOrListList = Array | list[list[int]]


def calculate_h0(labels_: ArrayOrList, n_docs: int, recall_target: float = .95, bias: float = 1.) -> float | None:
    """
    Calculates a p-score for our null hypothesis h0, that we have missed our recall target `recall_target`.

    :param labels_: An ordered sequence of 1s and 0s representing, in the order
        in which they were screened, relevant and irrelevant documents
        respectively.
    :param n_docs: The total number of documents from which you want to find the
        relevant examples. The size of the haystack.
    :param recall_target: The proportion of truly relevant documents you want
        to find, defaults to 0.95
    :param bias: The assumed likelihood of drawing a random relevant document
        over the likelihood of drawing a random irrelevant document. The higher
        this is, the better our ML has worked. When this is different to 1,
        we calculate the p score using biased urns.
    :return: p-score for our null hypothesis.
             We can reject the null hypothesis (and stop screening) if p is below 1 - our confidence level.

    """
    labels: npt.NDArray[np.int_] = (labels_ if type(labels_) is np.ndarray
                                    else np.array(labels_, dtype=np.int_))

    # Number of relevant documents we have seen
    r_seen = labels.sum()

    # Reverse the list so we can later construct the urns
    urns = labels[::-1]  # Urns of previous 1,2,...,N documents
    urn_sizes = np.arange(urns.shape[0]) + 1  # The sizes of these urns

    # Now we calculate k_hat, which is the minimum number of documents there would have to be
    # in each of our urns for the urn to be in keeping with our null hypothesis
    # that we have missed our target
    k_hat = np.floor(
        r_seen / recall_target + 1 -  # Divide num of relevant documents by our recall target and add 1  # noqa
        (
                r_seen -  # from this we subtract the total relevant documents seen  # noqa
                urns.cumsum()  # before each urn
        )
    )

    # Test the null hypothesis that a given recall target has been missed
    p: npt.NDArray[np.float64]
    if bias == 1:
        p = hypergeom.cdf(  # the probability of observing
            urns.cumsum(),  # the number of relevant documents in the sample
            n_docs - (urns.shape[0] - urn_sizes),  # In a population made up out of the urn and all remaining docs
            k_hat,  # Where K_hat docs in the population are actually relevant
            urn_sizes  # After observing this many documents
        )
    else:
        p = nchypergeom_wallenius.cdf(
            urns.cumsum(),  # the number of relevant documents in the sample
            n_docs - (urns.shape[0] - urn_sizes),  # In a population made up out of the urn and all remaining docs
            k_hat,  # Where K_hat docs in the population are actually relevant
            urn_sizes,  # After observing this many documents
            bias  # Where we are bias times more likely to pick a random relevant document
        )

    # We computed this for all, so only return the smallest
    p_min: float = p.min()

    if np.isnan(p_min):
        return None
    return p_min


def calculate_h0s(labels_: ArrayOrList,
                  n_docs: int,
                  recall_target: float = .95,
                  bias: float = 1.,
                  batch_size: int = 100) -> Iterator[tuple[int, float | None]]:
    """
    Calculates the p-score for H0 after each set of `batch_size` labels.

    :param labels_: 1D array of 0s (exclude) and 1s (include) screening annotations
    :param n_docs:Number of documents in the corpus (incl unseen)
    :param recall_target:
    :param bias:
    :param batch_size: H0 will be calculated after each batch
    :return:
    """
    labels: Array = (labels_ if type(labels_) is np.ndarray
                     else np.array(labels_, dtype=np.int_))
    n_seen = labels.shape[0]
    for n_seen_batch in range(batch_size, n_seen, batch_size):
        batch_labels = labels[:n_seen_batch]
        p_h0 = calculate_h0(batch_labels, n_docs=n_docs, bias=bias, recall_target=recall_target)
        yield n_seen_batch, p_h0

        if p_h0 is not None and p_h0 < (1.0 - recall_target):
            break
    else:  # Called, when we didn't break (did not meet the target)
        # There might be one more step if n_seen is not a multiple of batch_size
        p_h0 = calculate_h0(labels, n_docs=n_docs, bias=bias, recall_target=recall_target)
        yield n_seen, p_h0


def calculate_h0s_for_batches(labels: list[list[int]],
                              n_docs: int,
                              recall_target: float = .95,
                              bias: float = 1.) -> Iterator[tuple[int, float | None]]:
    """
    Calculates the p-score for H0 after each batch of labels.
    Similar to `calculate_h0s`, but we assume that batches are determined beforehand
    and we receive the batched sequence of annotations. This is useful, e.g. when batches are
    based on assignment scopes in the platform.

    :param labels: array of arrays of 0s (exclude) and 1s (include) screening annotations
    :param n_docs:
    :param recall_target:
    :param bias:
    :return:
    """
    pos = 0
    seen_labels = []
    for batch_labels in labels:
        seen_labels += batch_labels
        p_h0: float | None = calculate_h0(seen_labels, n_docs=n_docs, bias=bias, recall_target=recall_target)

        pos += len(batch_labels)
        yield pos, p_h0


def calculate_stopping_metric_for_batches(labels: list[list[int]],
                                          n_docs: int,
                                          recall_target: float = .95,
                                          bias: float = 1.) -> tuple[H0Series, list[float | None]]:
    """
    Calculates the p-score for H0 after each batch of labels  and recall after each label.

    :param labels: array of arrays of 0s (exclude) and 1s (include) screening annotations
    :param n_docs:
    :param recall_target:
    :param bias:
    :return:
    """
    p_h0s: H0Series = list(calculate_h0s_for_batches(labels=labels,
                                                     recall_target=recall_target,
                                                     bias=bias,
                                                     n_docs=n_docs))

    return p_h0s, compute_recall(labels[-1])


def calculate_stopping_metric(labels_: ArrayOrList,
                              n_docs: int,
                              recall_target: float = .95,
                              bias: float = 1.,
                              batch_size: int = 100) -> tuple[H0Series, list[float | None]]:
    """
    Calculates the p-score for H0 after each set of `batch_size` labels and recall after each label.

    :param labels_: 1D array of 0s (exclude) and 1s (include) screening annotations
    :param n_docs:Number of documents in the corpus (incl unseen)
    :param recall_target:
    :param bias:
    :param batch_size: H0 will be calculated after each batch
    :return:
    """
    labels: Array = (labels_ if type(labels_) is np.ndarray
                     else np.array(labels_, dtype=np.int_))

    p_h0s: H0Series = list(calculate_h0s(labels_=labels, batch_size=batch_size, bias=bias,
                                         n_docs=n_docs, recall_target=recall_target))

    return p_h0s, compute_recall(labels)


def compute_recall(labels_: ArrayOrList) -> list[float | None]:
    """
    Takes 1D list of integers (or np.array) and returns recall at each label.
    :param labels_:
    :return:
    """
    labels: Array = (labels_ if type(labels_) is np.ndarray
                     else np.array(labels_, dtype=np.int_))
    n_seen_relevant = labels.sum()
    recall: npt.NDArray[np.float64] = labels.cumsum() / n_seen_relevant
    recall_lst: list[float] = recall.tolist()
    return [ri if not np.isnan(ri) else None for ri in recall_lst]


def recall_frontier(
        labels_: ArrayOrList,
        n_docs: int,
        bias: float = 1.0,
        max_iter: int = 150,
) -> tuple[list[float], list[float]]:
    """
    Calculates a p-score for our null hypothesis h0, that we have missed our recall target `recall_target`, across a range of recall_targets.

    :param labels_: An ordered sequence of 1s and 0s representing, in the order
        in which they were screened, relevant and irrelevant documents
        respectively.
    :param n_docs: The total number of documents from which you want to find the
        relevant examples. The size of the haystack.
    :param bias: The assumed likelihood of drawing a random relevant document
        over the likelihood of drawing a random irrelevant document. The higher
        this is, the better our ML has worked. When this is different to 1,
        we calculate the p score using biased urns.
    :param max_iter: Fuse to prevent endless loop
    :return: A dictionary containing a list of recall targets: `recall_target`.
        alongside a list of p-scores: `p`.
    """

    recall_target = 0.99
    recall_targets: list[float] = []
    p_scores: list[float] = []
    it = 0
    while recall_target > 0:
        it += 1
        if it > max_iter:
            break
        p = calculate_h0(labels_, n_docs, recall_target, bias)

        if p is not None:
            p_scores.append(p)
            recall_targets.append(recall_target)

        recall_target -= 0.005

    return recall_targets, p_scores


def retrospective_h0(
        labels_: ArrayOrList,
        n_docs: int,
        recall_target: float = 0.95,
        bias: float = 1.,
        batch_size: int = 1000,
        confidence_level: float = 0.95
) -> tuple[list[int], list[float]]:
    """
    Calculates a p-score for our null hypothesis h0, that we have missed our recall target `recall_target`, every `batch_size` documents

    :param labels_: An ordered sequence of 1s and 0s representing, in the order
        in which they were screened, relevant and irrelevant documents
        respectively.
    :param n_docs: The total number of documents from which you want to find the
        relevant examples. The size of the haystack.
    :param recall_target: The proportion of truly relevant documents you want
        to find, defaults to 0.95
    :param bias: The assumed likelihood of drawing a random relevant document
        over the likelihood of drawing a random irrelevant document. The higher
        this is, the better our ML has worked. When this is different to 1,
        we calculate the p score using biased urns.
    :param batch_size: The size of the batches for which we will calculate our
        stopping criteria. Smaller batches = greater granularity = more
        computation time.
    :param confidence_level: The score will be calculated until p is smaller
        than 1-`confidence_level`
    :return: A dictionary containing a list of batch sizes: `batch_sizes`.
        alongside a list of p-scores: `p`.
    """
    labels: npt.NDArray[np.int_] = (labels_ if type(labels_) is np.ndarray
                                    else np.array(labels_, dtype=np.int8))

    n_seen_batch: list[int] =[]
    batch_ps: list[float] = []

    for n_seen in list(range(0, labels.shape[0], batch_size))[1:] +[ labels.shape[0]]:
        batch_labels = labels[:n_seen]
        p = calculate_h0(batch_labels, n_docs=n_docs, recall_target=recall_target, bias=bias)
        if p is not None:
            n_seen_batch.append(n_seen)
            batch_ps.append(p)

    return n_seen_batch, batch_ps
