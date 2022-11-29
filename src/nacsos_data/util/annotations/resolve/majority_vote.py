import uuid
from collections import Counter

from nacsos_data.models.annotations import \
    AnnotationValue, \
    FlattenedAnnotationSchemeLabel, \
    AnnotationScalarValueField, \
    AnnotationListValueField, \
    AnnotationModel, AnnotationSchemeLabelTypes
from nacsos_data.models.bot_annotations import AnnotationCollection, BotAnnotationModel, Label, GroupedBotAnnotation


def _majority_vote_list(label_annotations: list[AnnotationModel],
                        field: AnnotationListValueField) -> AnnotationValue:
    # form a union of all available sets of multi-labels (ignoring null values)
    flat_values = [li
                   for la in label_annotations
                   if la is not None and la.__dict__[field] is not None
                   for li in la.__dict__[field]
                   if li is not None]
    if len(flat_values) == 0:
        raise ValueError('Implausible input data (empty list of labels after union)')

    # FIXME: mypy error: Keywords must be strings  [misc]
    # FIXME: mypy error: Argument 1 to "AnnotationValue" has incompatible type
    #                    "**Dict[Literal['multi_int'], List[Any]]"; expected "Optional[bool]"  [arg-type]
    # FIXME: mypy error: Argument 1 to "AnnotationValue" has incompatible type
    #                    "**Dict[Literal['multi_int'], List[Any]]"; expected "Optional[int]"  [arg-type]
    # FIXME: mypy error: Argument 1 to "AnnotationValue" has incompatible type
    #                    "**Dict[Literal['multi_int'], List[Any]]"; expected "Optional[float]"  [arg-type]
    # FIXME: mypy error: Argument 1 to "AnnotationValue" has incompatible type
    #                    "**Dict[Literal['multi_int'], List[Any]]"; expected "Optional[str]"  [arg-type]
    return AnnotationValue(**{field: list(set(flat_values))})  # type: ignore[misc, arg-type]


def _majority_vote_scalar(label_annotations: list[AnnotationModel],
                          field: AnnotationScalarValueField) -> AnnotationValue:
    flat_values = [la.__dict__[field]
                   for la in label_annotations
                   if la is not None and la.__dict__[field] is not None]
    if len(flat_values) == 0:
        raise ValueError('Implausible input data (empty list of labels after plucking)')

    # FIXME: mypy error: Keywords must be strings  [misc]
    return AnnotationValue(**{field: Counter(flat_values).most_common()[0][0]})  # type: ignore[misc]


def _label_to_str(label: list[Label]) -> str:
    return ','.join([f'{li.key}-{li.repeat}' for li in label])


def naive_majority_vote(collection: AnnotationCollection,
                        scheme: list[FlattenedAnnotationSchemeLabel]) -> dict[str, list[GroupedBotAnnotation]]:
    scheme_lookup: dict[str, FlattenedAnnotationSchemeLabel] = {label.key: label for label in scheme}

    ret: dict[str, list[GroupedBotAnnotation]] = {}
    for item_id, grouped_annotations in collection.annotations.items():
        ret[item_id] = []
        parent_lookup: dict[str, str] = {}
        for label, annotations in grouped_annotations:
            kind: AnnotationSchemeLabelTypes = scheme_lookup[label[0].key].kind
            if kind == 'bool':
                value = _majority_vote_scalar(annotations, field='value_bool')
            elif kind == 'single':
                value = _majority_vote_scalar(annotations, field='value_int')
            elif kind == 'multi':
                value = _majority_vote_list(annotations, field='multi_int')
            # elif column_schemes[label_i].kind == 'int':
            #     pass
            # elif column_schemes[label_i].kind == 'float':
            #     pass
            # elif column_schemes[label_i].kind == 'str':
            #     pass
            # elif column_schemes[label_i].kind == 'intext':
            #     pass
            else:
                raise NotImplementedError(f'Majority vote for {kind} not implemented')

            ba_uuid = uuid.uuid4()
            parent_lookup[_label_to_str(label)] = str(ba_uuid)
            ret[item_id].append(GroupedBotAnnotation(
                path=label,
                annotation=BotAnnotationModel(bot_annotation_id=ba_uuid, item_id=item_id,
                                              key=label[0].key, repeat=label[0].repeat,
                                              **value.dict())))

        # Second loop to back-fill parents
        # NOTICE: This does *not* check for validity!
        #         (e.g. the parent might have been resolved to a choice where the current sub-label is not a child)
        for i, (label, annotations) in enumerate(grouped_annotations):
            if len(label) > 1:
                ret[item_id][i].annotation.parent = parent_lookup.get(_label_to_str(label[1:]), None)

    return ret
