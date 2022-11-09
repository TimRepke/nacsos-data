from collections import Counter

from nacsos_data.models.annotations import \
    AnnotationValue, \
    FlattenedAnnotationSchemeLabel, \
    AnnotationScalarValueField, \
    AnnotationListValueField
from nacsos_data.models.bot_annotations import AnnotationMatrix


def _majority_vote_list(label_annotations: list[AnnotationValue | None] | None,
                        field: AnnotationListValueField) -> AnnotationValue | None:
    # form a union of all available sets of multi-labels (ignoring null values)
    if label_annotations is None:
        return None

    flat_values = [li
                   for la in label_annotations
                   if la is not None and la.__dict__[field] is not None
                   for li in la.__dict__[field]
                   if li is not None]
    if len(flat_values) == 0:
        return None

    return AnnotationValue(**{field: list(set(flat_values))})


def _majority_vote_scalar(label_annotations: list[AnnotationValue | None] | None,
                          field: AnnotationScalarValueField) -> AnnotationValue | None:
    if label_annotations is None:
        return None

    flat_values = [la.__dict__[field]
                   for la in label_annotations
                   if la is not None and la.__dict__[field] is not None]
    if len(flat_values) == 0:
        return None

    return AnnotationValue(**{field: Counter(flat_values).most_common()[0][0]})


def naive_majority_vote(matrix: AnnotationMatrix,
                        scheme: list[FlattenedAnnotationSchemeLabel]) -> dict[str, list[AnnotationValue | None]]:
    """

    :return:
    """

    scheme_lookup: dict[str, FlattenedAnnotationSchemeLabel] = {
        label.key: label
        for label in scheme
    }
    column_schemes: list[FlattenedAnnotationSchemeLabel] = [
        scheme_lookup[label[-1][0]]
        for label in matrix.labels
    ]
    ret: dict[str, list[AnnotationValue | None]] = {}
    for item_id, item_annotations in matrix.matrix.items():  # type: str, list[list[AnnotationValue | None] | None]
        ret[item_id] = [None] * len(matrix.labels)
        for label_i, label_annotations in enumerate(item_annotations):
            if label_annotations is not None:
                if column_schemes[label_i].kind == 'bool':
                    ret[item_id][label_i] = _majority_vote_scalar(label_annotations, field='value_bool')
                elif column_schemes[label_i].kind == 'int':
                    ret[item_id][label_i] = _majority_vote_scalar(label_annotations, field='value_int')
                elif column_schemes[label_i].kind == 'single':
                    ret[item_id][label_i] = _majority_vote_scalar(label_annotations, field='value_int')
                elif column_schemes[label_i].kind == 'multi':
                    ret[item_id][label_i] = _majority_vote_list(label_annotations, field='multi_int')
                # elif column_schemes[label_i].kind == 'float':
                #     pass
                # elif column_schemes[label_i].kind == 'str':
                #     pass
                # elif column_schemes[label_i].kind == 'intext':
                #     pass
                else:
                    raise NotImplementedError(f'Majority vote for {column_schemes[label_i].kind} not implemented')

    return ret
