from typing import Iterator

from scipy.stats import hypergeom
import numpy as np
import numpy.typing as npt

from nacsos_data.models.annotation_tracker import H0Series


def calculate_h0(labels_: npt.ArrayLike, n_docs: int, recall_target: float = .95) -> float:
    """
    TL;DR: Calculate stopping criterion at this point in time.

    For more information, consult the MWE for BUSCAR:
       https://github.com/mcallaghan/rapid-screening/blob/master/analysis/minimal_example.ipynb

    :param labels_: 1D array of 0s (exclude) and 1s (include) screening annotations
    :param recall_target:
    :param n_docs: Number of documents in the corpus (incl unseen)
    :return:
    """
    labels: npt.NDArray[np.int_] = (labels_ if type(labels_) is np.ndarray
                                    else np.array(labels_, dtype=np.int_))

    # Number of relevant documents we have seen
    r_seen = labels.sum()
    print(r_seen)
    # Reverse the list so we can later construct the urns
    urns = labels[::-1]  # Urns of previous 1,2,...,N documents
    urn_sizes = np.arange(urns.shape[0]) + 1  # The sizes of these urns
    print(urns)
    print(urn_sizes)
    # Now we calculate k_hat, which is the minimum number of documents there would have to be
    # in each of our urns for the urn to be in keeping with our null hypothesis
    # that we have missed our target
    k_hat = np.floor(
        r_seen / recall_target + 1 -  # Divide num of relevant documents by our recall target and add 1  # noqa: W504
        (
                r_seen -  # from this we subtract the total relevant documents seen  # noqa: W504
                urns.cumsum()  # before each urn
        )
    )
    print(k_hat)
    print(urns.cumsum())
    print(n_docs - (urns.shape[0] - urn_sizes))
    # Test the null hypothesis that a given recall target has been missed
    p: npt.NDArray[np.float_] = hypergeom.cdf(  # the probability of observing
        urns.cumsum(),  # the number of relevant documents in the sample
        n_docs - (urns.shape[0] - urn_sizes),  # in a population made up out of the urn and all remaining docs
        k_hat,  # where K_hat docs in the population are actually relevant
        urn_sizes  # after observing this many documents
    )

    print(p)

    # We computed this for all, so only return the smallest
    p_min: float = p.min()
    return p_min


def calculate_h0s(labels_: npt.ArrayLike,
                  n_docs: int,
                  recall_target: float = .95,
                  batch_size: int = 100) -> Iterator[tuple[int, float]]:
    """
    Calculates the p-score for H0 after each set of `batch_size` labels.

    :param labels_: 1D array of 0s (exclude) and 1s (include) screening annotations
    :param n_docs:Number of documents in the corpus (incl unseen)
    :param recall_target:
    :param batch_size: H0 will be calculated after each batch
    :return:
    """
    labels: npt.NDArray[np.int_] = (labels_ if type(labels_) is np.ndarray
                                    else np.array(labels_, dtype=np.int_))
    n_seen = labels.shape[0]
    for n_seen_batch in range(batch_size, n_seen, batch_size):
        batch_labels = labels[:n_seen_batch]
        p_h0 = calculate_h0(batch_labels, n_docs=n_docs, recall_target=recall_target)
        yield n_seen_batch, p_h0

        if p_h0 < (1.0 - recall_target):
            break
    else:  # Called, when we didn't break (did not meet the target)
        # There might be one more step if n_seen is not a multiple of batch_size
        p_h0 = calculate_h0(labels, n_docs=n_docs, recall_target=recall_target)
        yield n_seen, p_h0


def calculate_h0s_for_batches(labels: npt.ArrayLike,
                              n_docs: int,
                              recall_target: float = .95) -> Iterator[tuple[int, float | None]]:
    """
    Calculates the p-score for H0 after each batch of labels.
    Similar to `calculate_h0s`, but we assume that batches are determined beforehand
    and we receive the batched sequence of annotations. This is useful, e.g. when batches are
    based on assignment scopes in the platform.

    :param labels_: array of arrays of 0s (exclude) and 1s (include) screening annotations
    :param n_docs:
    :param recall_target:
    :return:
    """
    pos = 0
    for batch_labels in labels:
        p_h0 = calculate_h0(batch_labels, n_docs=n_docs, recall_target=recall_target)

        if np.isnan(p_h0):
            p_h0 = None

        pos += len(batch_labels)
        yield pos, p_h0

        if p_h0 is not None and p_h0 < (1.0 - recall_target):
            break


def calculate_stopping_metric_for_batches(labels: npt.ArrayLike,
                                          n_docs: int,
                                          recall_target: float = .95) -> tuple[H0Series, list[float]]:
    """
    Calculates the p-score for H0 after each batch of labels  and recall after each label.

    :param labels_: array of arrays of 0s (exclude) and 1s (include) screening annotations
    :param n_docs:
    :param recall_target:
    :return:
    """
    p_h0s: H0Series = list(calculate_h0s_for_batches(labels=labels,
                                                     recall_target=recall_target,
                                                     n_docs=n_docs))

    return p_h0s, compute_recall(labels[-1])


def calculate_stopping_metric(labels_: npt.ArrayLike,
                              n_docs: int,
                              recall_target: float = .95,
                              batch_size: int = 100) -> tuple[H0Series, list[float]]:
    """
    Calculates the p-score for H0 after each set of `batch_size` labels and recall after each label.

    :param labels_: 1D array of 0s (exclude) and 1s (include) screening annotations
    :param n_docs:Number of documents in the corpus (incl unseen)
    :param recall_target:
    :param batch_size: H0 will be calculated after each batch
    :return:
    """
    labels: npt.NDArray[np.int_] = (labels_ if type(labels_) is np.ndarray
                                    else np.array(labels_, dtype=np.int_))

    p_h0s: H0Series = list(calculate_h0s(labels_=labels, batch_size=batch_size,
                                         n_docs=n_docs, recall_target=recall_target))

    return p_h0s, compute_recall(labels)


def compute_recall(labels_: npt.ArrayLike) -> list[float | None]:
    """
    Takes 1D list of integers (or np.array) and returns recall at each label.
    :param labels_:
    :return:
    """
    labels: npt.NDArray[np.int_] = (labels_ if type(labels_) is np.ndarray
                                    else np.array(labels_, dtype=np.int_))
    n_seen_relevant = labels.sum()
    recall: npt.NDArray[np.float_] = labels.cumsum() / n_seen_relevant
    recall_lst: list[float] = recall.tolist()
    return [ri if not np.isnan(ri) else None for ri in recall_lst]
