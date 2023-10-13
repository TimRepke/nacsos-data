import logging
from collections import Counter

from nacsos_data.util.annotations.validation import (
    FlatLabel,
    resolve_bot_annotation_parents,
    same_values,
    has_values
)
from nacsos_data.util.errors import EmptyAnnotationsError
from nacsos_data.models.annotations import (
    AnnotationValue,
    AnnotationScalarValueField,
    AnnotationListValueField,
    AnnotationSchemeLabelTypes,
    ItemAnnotation
)
from nacsos_data.models.bot_annotations import (  # noqa: F401
    ResolutionMatrix,
    ResolutionCell,
    ResolutionStatus
)

logger = logging.getLogger('nacsos_data.util.annotations.resolve')


def _majority_vote_list(label_annotations: list[ItemAnnotation],
                        field: AnnotationListValueField,
                        label: FlatLabel) -> AnnotationValue:
    # form a union of all available sets of multi-labels (ignoring null values)
    flat_values = [li
                   for la in label_annotations
                   if la is not None and la.__dict__[field] is not None
                   for li in la.__dict__[field]
                   if li is not None]

    # logger.debug(f'Found {len(flat_values)} annotations for "{label.path_key}" of type "{field}".')

    if len(flat_values) == 0:
        raise EmptyAnnotationsError(f'No entries for "{label.path_key}" of type "{field}" '
                                    f'(empty list of labels after union).')

    return AnnotationValue(**{field: list(set(flat_values))})  # type: ignore[misc, arg-type]


def _majority_vote_scalar(label_annotations: list[ItemAnnotation],
                          field: AnnotationScalarValueField,
                          label: FlatLabel) -> AnnotationValue:
    flat_values = [la.__dict__[field]
                   for la in label_annotations
                   if la is not None and la.__dict__[field] is not None]

    # logger.debug(f'Found {len(flat_values)} annotations for "{label.path_key}" of type "{field}".')

    if len(flat_values) == 0:
        raise EmptyAnnotationsError(f'No entries for "{label.path_key}" of type "{field}".')

    return AnnotationValue(**{field: Counter(flat_values).most_common()[0][0]})  # type: ignore[misc]


def _majority_vote_str(label_annotations: list[ItemAnnotation],
                       field: AnnotationScalarValueField,
                       label: FlatLabel) -> AnnotationValue:
    flat_values: list[str] = [la.__dict__[field]
                              for la in label_annotations
                              if la is not None and la.__dict__[field] is not None and len(la.__dict__[field]) > 0]

    # logger.debug(f'Found {len(flat_values)} annotations for "{label.path_key}" of type "{field}".')

    if len(flat_values) == 0:
        raise EmptyAnnotationsError(f'No entries for "{label.path_key}" of type "{field}".')

    return AnnotationValue(**{field: '\n----\n'.join(flat_values)})  # type: ignore[arg-type, misc]


def naive_majority_vote(annotation_map: ResolutionMatrix,
                        label_map: dict[str, FlatLabel],
                        fix_parent_references: bool = True) -> ResolutionMatrix:
    for row_key, row in annotation_map.items():
        cell: ResolutionCell
        label_key: str
        for label_key, cell in row.items():
            annotations = [entry.annotation
                           for labels in cell.labels.values()
                           for entry in labels
                           if entry.annotation and has_values(entry.annotation)]

            # No annotations to resolve, skipping the rest...
            if len(annotations) == 0:
                continue

            label = label_map[label_key]
            kind: AnnotationSchemeLabelTypes = label.kind

            try:
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

                if same_values(value, cell.resolution):
                    pass
                else:
                    if not has_values(cell.resolution):
                        cell.status = ResolutionStatus.NEW
                    else:
                        cell.status = ResolutionStatus.CHANGED

                    cell.resolution.value_bool = value.value_bool  # type: ignore[assignment]
                    cell.resolution.value_int = value.value_int  # type: ignore[assignment]
                    cell.resolution.value_str = value.value_str  # type: ignore[assignment]
                    cell.resolution.value_float = value.value_float  # type: ignore[assignment]
                    cell.resolution.multi_int = value.multi_int  # type: ignore[assignment]
            except EmptyAnnotationsError as e:
                logger.debug(e)

    if fix_parent_references:
        resolve_bot_annotation_parents(annotation_map, label_map)

    return annotation_map
