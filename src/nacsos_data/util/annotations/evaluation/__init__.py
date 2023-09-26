def get_new_label_batches(old_seq: list[list[int]], new_seq: list[list[int]]) -> list[list[int]]:
    """
    When performing updates to evaluation metrics, you sometimes only want to focus on the new data.
    This method compares the two sequences for validity (only differences at the end of the sequence) and
    returns the difference at the end of `new_seq` (all new batches and, if applicable,
    the completion of the previously last batch).

    :param old_seq:
    :param new_seq:
    :return:
    """
    if len(old_seq) > len(new_seq):  # We have fewer batches than before
        raise ValueError('The old sequence should never be longer than the new sequence of labels!')

    if len(old_seq) < len(new_seq):  # We have more batches than before
        for old_batch, new_batch in zip(old_seq[:-1], new_seq[:len(old_seq) - 1]):
            if old_batch != new_batch:
                raise ValueError('Labels in the beginning of the sequence changed, please recompute fully.')
    # We have the same number of batches than before (or more)
    # Compare the last one, because it would be fine to have changed
    last_old_seq = old_seq[-1]
    last_new_seq = new_seq[len(old_seq) - 1]
    if len(last_old_seq) > len(last_new_seq):  # Last batch has smaller respective new batch
        raise ValueError('The old sequence should never be longer than the new sequence of labels!')
    # Check if labels are fine up to the length of the last batch of the old data
    for old_batch_v, new_batch_v in zip(last_old_seq, last_new_seq):
        if old_batch_v != new_batch_v:
            raise ValueError('Labels in the beginning of the sequence changed, please recompute fully.')

    new_data: list[list[int]] = []
    # The last batch from the old data got longer, so add the rest of that to our diff set
    if len(last_old_seq) < len(last_new_seq):
        new_data.append(last_new_seq[len(last_old_seq):])

    # We have the same number of batches in both sets, so nothing else to do
    # if len(old_seq) == len(new_seq):

    # Add additional batches from the new sequence to the diff
    if len(old_seq) < len(new_seq):
        new_data += new_seq[len(old_seq):]

    return new_data
