from uuid import UUID, uuid4
import logging

from nacsos_data.models.annotations import (
    AnnotationModel,
    AnnotationSchemeModel,
    FlattenedAnnotationSchemeLabel,
    AssignmentStatus,
    AnnotationSchemeLabel,
    AnnotationSchemeModelFlat,
    AnnotationSchemeLabelChoiceFlat,
    FlatLabelChoice,
    FlatLabel,
    Label,
    AnnotationValue
)
from nacsos_data.models.bot_annotations import ResolutionMatrix, ResolutionCell  # noqa: F401

logger = logging.getLogger('nacsos_data.annotation.validation')


def labels_from_scheme(scheme: AnnotationSchemeModel,
                       ignore_hierarchy: bool = False,
                       ignore_repeat: bool = False,
                       keys: list[str] | None = None,
                       repeats: list[int] | None = None) -> list[FlatLabel]:
    """
    This method generates all possible label paths from the annotation scheme.
    It respects the applied filters to exclude paths that would not exist under these conditions.

    :param scheme: annotation scheme to generate the labels from
    :param ignore_hierarchy: will not include nesting of labels (all paths will have length 1)
    :param ignore_repeat: will not generate multiples for primary, secondary, ... labels
    :param keys: if not None, include only these label keys and skip all others (ignores hierarchy!)
    :param repeats: if not None, include only these repeats and skip all others
    :return:
    """
    run = 0

    def recurse(labels: list[AnnotationSchemeLabel],
                prefix: list[Label], parent: int | None = None,
                parent_key: str | None = None, parent_value: int | None = None) -> list[FlatLabel]:
        nonlocal run
        ret = []
        for label in labels:
            max_repeat = label.max_repeat
            if keys is not None and label.key not in keys:
                max_repeat = 1
            for repeat in range(1 if ignore_repeat else max_repeat):
                if repeats is None or (repeat + 1) in repeats:
                    if keys is None or label.key in keys:
                        run += 1
                        path = [Label(key=label.key, repeat=repeat + 1)] + prefix
                        ret.append(FlatLabel(path=path,
                                             path_key=path_to_string(path),
                                             repeat=repeat + 1,
                                             parent_int=parent,
                                             parent_key=parent_key,
                                             parent_value=parent_value,
                                             name=label.name,
                                             hint=label.hint,
                                             key=label.key,
                                             required=label.required,
                                             max_repeat=label.max_repeat,
                                             kind=label.kind,
                                             choices=[FlatLabelChoice(**c.model_dump())
                                                      for c in label.choices]
                                             if label.choices is not None else None))
                    if label.choices and (ignore_hierarchy or keys is None or label.key in keys):
                        next_parent = run - 1
                        for choice in label.choices:
                            if choice.children is not None and len(choice.children) > 0:
                                if not ignore_hierarchy:
                                    next_prefix = [Label(key=label.key, repeat=repeat + 1, value=choice.value)]
                                else:
                                    next_prefix = []
                                sublabels = recurse(labels=choice.children,
                                                    prefix=next_prefix + prefix,
                                                    parent=next_parent,
                                                    parent_key=path_to_string(next_prefix),
                                                    parent_value=choice.value)
                                ret += sublabels
        return ret

    result = recurse(scheme.labels, prefix=[], parent=None)
    return result


def path_to_string(path: list[Label]) -> str:
    return '|'.join([f'{pl.key}-{pl.repeat}' for pl in path])


def resolve_bot_annotation_parents(annotation_map: ResolutionMatrix,
                                   label_map: dict[str, FlatLabel]) -> ResolutionMatrix:
    # Loop to back-fill parent_ids
    # NOTICE: This does *not* check for validity!
    #         (e.g. the parent might have been resolved to a choice where the current sub-label is not a child)
    for row_key, row in annotation_map.items():
        label_key: str
        cell: ResolutionCell
        for label_key, cell in row.items():
            if cell.resolution is not None and label_key in row:
                parent_key = label_map[label_key].parent_key
                if parent_key is not None and parent_key in row:
                    parent = row[parent_key].resolution
                    if parent is not None:
                        cell.resolution.parent = parent.bot_annotation_id
                    else:
                        logger.debug(  # type: ignore[unreachable]
                            f'Looks like I have no parent for {row_key} -> {label_key}')
    return annotation_map


def flatten_annotation_scheme(annotation_scheme: AnnotationSchemeModel) -> AnnotationSchemeModelFlat:
    def recurse(labels: list[AnnotationSchemeLabel],
                parent_choice: int | None = None,
                parent_label: str | None = None,
                parent_repeat: int = 1) -> list[FlattenedAnnotationSchemeLabel]:
        ret = []

        choices: list[AnnotationSchemeLabelChoiceFlat] | None
        for label in labels:
            if label.choices is None:
                choices = None
            else:
                choices = []
                for choice in label.choices:
                    choices.append(AnnotationSchemeLabelChoiceFlat(name=choice.name,
                                                                   hint=choice.hint,
                                                                   value=choice.value))
                    if choice.children is not None:
                        ret += recurse(choice.children,
                                       parent_choice=choice.value,
                                       parent_label=label.key,
                                       parent_repeat=label.max_repeat)

            ret.append(FlattenedAnnotationSchemeLabel(key=label.key,
                                                      name=label.name,
                                                      hint=label.hint,
                                                      required=label.required,
                                                      max_repeat=label.max_repeat,
                                                      implicit_max_repeat=label.max_repeat * parent_repeat,
                                                      kind=label.kind,
                                                      choices=choices,
                                                      parent_label=parent_label,
                                                      parent_choice=parent_choice))

        return ret

    return AnnotationSchemeModelFlat(annotation_scheme_id=annotation_scheme.annotation_scheme_id,
                                     project_id=annotation_scheme.project_id,
                                     name=annotation_scheme.name,
                                     description=annotation_scheme.description,
                                     labels=recurse(annotation_scheme.labels))


AnnotationModelLookupType = dict[str, list[AnnotationModel]]


def create_annotations_lookup(annotations: list[AnnotationModel]) -> AnnotationModelLookupType:
    annotations_map: AnnotationModelLookupType = {}
    for annotation in annotations:
        if annotation.key not in annotations_map:
            annotations_map[annotation.key] = []
        annotations_map[annotation.key].append(annotation)

    for key in annotations_map.keys():
        annotations_map[key] = sorted(annotations_map[key], key=lambda a: a.repeat)  # type: ignore[no-any-return]

    return annotations_map


def validate_annotated_assignment(annotation_scheme: AnnotationSchemeModel,
                                  annotations: list[AnnotationModel]) -> AssignmentStatus:
    if annotations is None or len(annotations) == 0:
        return AssignmentStatus.OPEN

    annotations_map = create_annotations_lookup(annotations)

    def recurse(labels: list[AnnotationSchemeLabel], repeat: int = 1, parent: str | UUID | None = None) \
            -> AssignmentStatus:
        status = AssignmentStatus.FULL

        for label in labels:
            if label.key not in annotations_map:
                if label.required:
                    status = AssignmentStatus.PARTIAL
            else:
                cnt = 0
                for annotation in annotations_map[label.key]:
                    if annotation.parent == parent:
                        cnt += 1
                        for ci, choice in enumerate(label.choices or []):
                            if choice.children is not None and choice.value == annotation.value_int:
                                child_state = recurse(choice.children,
                                                      repeat=repeat,
                                                      parent=annotation.annotation_id)
                                if child_state != AssignmentStatus.FULL:
                                    status = child_state
                if label.required and cnt == 0:
                    status = AssignmentStatus.PARTIAL
                if cnt > label.max_repeat:
                    logger.debug(f'{cnt} > {label.max_repeat}  || {label}')
                    status = AssignmentStatus.INVALID

        return status

    return recurse(annotation_scheme.labels)


def merge_scheme_and_annotations(annotation_scheme: AnnotationSchemeModel,
                                 annotations: list[AnnotationModel]) -> AnnotationSchemeModel:
    if annotations is None or len(annotations) == 0:
        return annotation_scheme

    annotations_map = create_annotations_lookup(annotations)

    def recurse(labels: list[AnnotationSchemeLabel], repeat: int = 1, parent: str | UUID | None = None) \
            -> list[AnnotationSchemeLabel]:
        ret = []

        for label in labels:
            if label.key not in annotations_map:
                ret.append(label)
            else:
                cnt = 0
                for annotation in annotations_map[label.key]:
                    if annotation.parent == parent:
                        cnt += 1
                        label_cpy = label.model_copy(deep=True)
                        label_cpy.annotation = annotation

                        for ci, choice in enumerate(label_cpy.choices or []):
                            if choice.children is not None:
                                choice.children = recurse(choice.children,
                                                          repeat=repeat,
                                                          parent=annotation.annotation_id)
                        ret.append(label_cpy)
        return ret

    annotation_scheme.labels = recurse(annotation_scheme.labels)
    return annotation_scheme


def has_annotation(label: AnnotationSchemeLabel) -> bool:
    return label.annotation is not None \
        and (label.annotation.value_int is not None  # type: ignore[unreachable]
             or label.annotation.value_str is not None
             or label.annotation.value_bool is not None
             or label.annotation.value_float is not None
             or label.annotation.multi_int is not None)


def has_values(anno: AnnotationValue) -> bool:
    return (anno.value_int is not None
            or anno.value_bool is not None
            or anno.value_float is not None
            or anno.value_str is not None
            or anno.multi_int is not None)


def same_values(anno_a: AnnotationValue, anno_b: AnnotationValue) -> bool:
    return not (anno_a.value_int != anno_b.value_int
                or anno_a.value_str != anno_b.value_str
                or anno_a.value_float != anno_b.value_float
                or anno_a.value_bool != anno_b.value_bool
                or (set(anno_a.multi_int or []) != set(anno_b.multi_int or [])))


def annotated_scheme_to_annotations(scheme: AnnotationSchemeModel) -> list[AnnotationModel]:
    ret = []

    def recurse(labels: list[AnnotationSchemeLabel], parent_id: str | UUID | None = None) -> None:
        for label in labels:
            if has_annotation(label):
                assert label.annotation is not None
                if label.annotation.annotation_id is None:
                    label.annotation.annotation_id = uuid4()
                label.annotation.parent = parent_id
                ret.append(label.annotation)

                if label.choices:
                    for choice in label.choices:
                        if choice.children:
                            recurse(choice.children, parent_id=label.annotation.annotation_id)

    recurse(scheme.labels)

    return ret
