import logging
import uuid
from collections import Counter

from nacsos_data.util.annotations.validation import FlatLabel
from nacsos_data.util.errors import EmptyAnnotationsError
from nacsos_data.models.annotations import \
    AnnotationValue, \
    AnnotationScalarValueField, \
    AnnotationListValueField, \
    AnnotationModel, \
    AnnotationSchemeLabelTypes
from nacsos_data.models.bot_annotations import BotAnnotationModel, GroupedBotAnnotation, \
    ResolutionMatrix, ResolutionCell

logger = logging.getLogger('nacsos_data.util.annotations.resolve')


def _majority_vote_list(label_annotations: list[AnnotationModel],
                        field: AnnotationListValueField,
                        label: FlatLabel) -> AnnotationValue:
    # form a union of all available sets of multi-labels (ignoring null values)
    flat_values = [li
                   for la in label_annotations
                   if la is not None and la.__dict__[field] is not None
                   for li in la.__dict__[field]
                   if li is not None]

    logger.debug(f'Found {len(flat_values)} annotations for "{label.path_key}" of type "{field}".')

    if len(flat_values) == 0:
        raise EmptyAnnotationsError(f'No entries for "{label.path_key}" of type "{field}" '
                                    f'(empty list of labels after union).')

    return AnnotationValue(**{field: list(set(flat_values))})  # type: ignore[misc, arg-type]


def _majority_vote_scalar(label_annotations: list[AnnotationModel],
                          field: AnnotationScalarValueField,
                          label: FlatLabel) -> AnnotationValue:
    flat_values = [la.__dict__[field]
                   for la in label_annotations
                   if la is not None and la.__dict__[field] is not None]

    logger.debug(f'Found {len(flat_values)} annotations for "{label.path_key}" of type "{field}".')

    if len(flat_values) == 0:
        raise EmptyAnnotationsError(f'No entries for "{label.path_key}" of type "{field}".')

    return AnnotationValue(**{field: Counter(flat_values).most_common()[0][0]})  # type: ignore[misc]


def _majority_vote_str(label_annotations: list[AnnotationModel],
                       field: AnnotationScalarValueField,
                       label: FlatLabel) -> AnnotationValue:
    flat_values: list[str] = [la.__dict__[field]
                              for la in label_annotations
                              if la is not None and la.__dict__[field] is not None and len(la.__dict__[field]) > 0]

    logger.debug(f'Found {len(flat_values)} annotations for "{label.path_key}" of type "{field}".')

    if len(flat_values) == 0:
        raise EmptyAnnotationsError(f'No entries for "{label.path_key}" of type "{field}".')

    return AnnotationValue(**{field: '\n----\n'.join(flat_values)})  # type: ignore[arg-type, misc]


def naive_majority_vote(annotation_map: ResolutionMatrix,
                        label_map: dict[str, FlatLabel]) -> ResolutionMatrix:
    for row_key, row in annotation_map.items():
        item_id = row_key.split('|')[1]
        for label_key, cell in row.items():  # type: str, ResolutionCell
            annotations = [entry.annotation for labels in cell.labels.values() for entry in labels]

            # No annotations to resolve, skipping the rest...
            if len(annotations) == 0:
                continue

            label = label_map[label_key]
            kind: AnnotationSchemeLabelTypes = label.kind

            if kind == 'bool':
                value = _majority_vote_scalar(annotations, field='value_bool', label=label)
            elif kind == 'single':
                value = _majority_vote_scalar(annotations, field='value_int', label=label)
            elif kind == 'multi':
                value = _majority_vote_list(annotations, field='multi_int', label=label)
            elif kind == 'str':
                value = _majority_vote_str(annotations, field='value_str', label=label)
            else:
                raise NotImplementedError(f'Majority vote for {kind} not implemented ({label})')

            ba_uuid = uuid.uuid4()
            cell.resolution = BotAnnotationModel(bot_annotation_id=ba_uuid, item_id=item_id,
                                                 key=label.key, repeat=label.repeat,
                                                 **value.model_dump())

        # Second loop to back-fill parent_ids
        # NOTICE: This does *not* check for validity!
        #         (e.g. the parent might have been resolved to a choice where the current sub-label is not a child)
        for label_key, cell in row.items():  # type: str, ResolutionCell
            if cell.resolution is not None:
                parent_key = label_map[label_key].parent_key
                if parent_key is not None:
                    parent = row[parent_key].resolution
                    if parent is not None:
                        cell.resolution.parent = parent.bot_annotation_id
                    else:
                        logger.debug(f'Looks like I have no parent for {row_key} -> {label_key}')

    return annotation_map
