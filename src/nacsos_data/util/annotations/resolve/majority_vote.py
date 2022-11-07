from collections import Counter

from nacsos_data.models.annotations import AnnotationValue, FlattenedAnnotationSchemeLabel
from nacsos_data.models.bot_annotations import AnnotationMatrix


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
                match column_schemes[label_i].kind:
                    case 'bool':
                        majority_label: bool = Counter([la.v_bool
                                                        for la in label_annotations
                                                        if la is not None and la.v_bool is not None]
                                                       ).most_common()[0][0]
                        ret[item_id][label_i] = AnnotationValue(v_bool=majority_label)
                    case 'int' | 'single':
                        majority_label: int = Counter([la.v_int  # type: ignore[no-redef]
                                                       for la in label_annotations
                                                       if la is not None and la.v_int is not None]
                                                      ).most_common()[0][0]
                        ret[item_id][label_i] = AnnotationValue(v_int=majority_label)
                    # case 'float':
                    #     ret[item_id][label_i] = AnnotationValue(v_float=majority_label)
                    # case 'str' | 'intext':
                    #     ret[item_id][label_i] = AnnotationValue(v_str=majority_label)
                    case _:
                        raise NotImplementedError(f'Majority vote for {column_schemes[label_i].kind} not implemented')

    return ret
