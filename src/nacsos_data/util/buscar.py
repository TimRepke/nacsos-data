from scipy.stats import hypergeom
import numpy as np
import numpy.typing as npt

H0Series = list[tuple[int, float]]


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

    # Reverse the list so we can later construct the urns
    urns = labels[::-1]  # Urns of previous 1,2,...,N documents
    urn_sizes = np.arange(urns.shape[0]) + 1  # The sizes of these urns

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

    # Test the null hypothesis that a given recall target has been missed
    p: npt.NDArray[np.float_] = hypergeom.cdf(  # the probability of observing
        urns.cumsum(),  # the number of relevant documents in the sample
        n_docs - (urns.shape[0] - urn_sizes),  # in a population made up out of the urn and all remaining docs
        k_hat,  # where K_hat docs in the population are actually relevant
        urn_sizes  # after observing this many documents
    )

    # We computed this for all, so only return the smallest
    p_min: float = p.min()
    return p_min


def calculate_h0s_batched(labels_: npt.ArrayLike,
                          n_docs: int,
                          recall_target: float = .95,
                          batch_size: int = 100) -> tuple[H0Series, list[float]]:
    """
    Calculates the p-score for H0 after each batch of labels.

    :param labels_: 1D array of 0s (exclude) and 1s (include) screening annotations
    :param n_docs:Number of documents in the corpus (incl unseen)
    :param recall_target:
    :param batch_size: H0 will be calculated after each batch
    :return:
    """
    labels: npt.NDArray[np.int_] = (labels_ if type(labels_) is np.ndarray
                                    else np.array(labels_, dtype=np.int_))

    n_seen = labels.shape[0]

    p_h0s: H0Series = []
    for n_seen_batch in range(batch_size, n_seen, batch_size):
        batch_labels = labels[:n_seen_batch]
        p_h0 = calculate_h0(batch_labels, n_docs=n_docs, recall_target=recall_target)
        p_h0s.append((n_seen_batch, p_h0))

        if p_h0 < (1.0 - recall_target):
            break
    else:  # Called, when we didn't break (did not meet the target)
        # There might be one more step if n_seen is not a multiple of batch_size
        p_h0 = calculate_h0(labels, n_docs=n_docs, recall_target=recall_target)
        p_h0s.append((n_seen, p_h0))

    n_seen_relevant = labels.sum()
    recall: npt.NDArray[np.float_] = labels.cumsum() / n_seen_relevant
    recall_lst: list[float] = recall.tolist()
    return p_h0s, recall_lst


def calculate_h0s_for_batches(labels_: npt.ArrayLike,
                              n_docs: int,
                              recall_target: float = .95) -> tuple[H0Series, list[float]]:
    """
    Calculates the p-score for H0 after each batch of labels.
    Similar to `calculate_h0s_batched`, but we assume that batches are determined beforehand
    and we receive the batched sequence of annotations. This is useful, e.g. when batches are
    based on assignment scopes in the platform.

    :param labels_: array of arrays of 0s (exclude) and 1s (include) screening annotations
    :param n_docs:
    :param recall_target:
    :return:
    """
    labels: npt.NDArray[np.int_] = (labels_ if type(labels_) is np.ndarray
                                    else np.array(labels_, dtype=np.int_))

    p_h0s: H0Series = []
    for batch_labels in labels:
        p_h0 = calculate_h0(batch_labels, n_docs=n_docs, recall_target=recall_target)
        p_h0s.append((len(batch_labels), p_h0))

        if p_h0 < (1.0 - recall_target):
            break

    n_seen_relevant = labels[-1].sum()
    recall: npt.NDArray[np.float_] = labels[-1].cumsum() / n_seen_relevant
    recall_lst: list[float] = recall.tolist()
    return p_h0s, recall_lst
