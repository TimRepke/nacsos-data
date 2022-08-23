from uuid import UUID, uuid4
import logging
from nacsos_data.models.annotations import \
    AnnotationModel, \
    AnnotationSchemeModel, \
    FlattenedAnnotationSchemeLabel, \
    AssignmentStatus, \
    AnnotationSchemeLabel

logger = logging.getLogger('nacsos_data.annotation.validation')


def flatten_annotation_scheme(annotation_scheme: AnnotationSchemeModel) -> list[FlattenedAnnotationSchemeLabel]:
    def recurse(labels: list[AnnotationSchemeLabel], parent_label: str = None,
                parent_repeat: int = 1) -> list[FlattenedAnnotationSchemeLabel]:
        ret = []

        for label in labels:
            if label.choices is None:
                choices = None
            else:
                choices = []
                for choice in label.choices:
                    choices.append(choice.value)
                    if choice.children is not None:
                        ret += recurse(choice.children,
                                       parent_label=label.key,
                                       parent_repeat=label.max_repeat)

            ret.append(FlattenedAnnotationSchemeLabel(key=label.key,
                                                      required=label.required,
                                                      max_repeat=label.max_repeat,
                                                      implicit_max_repeat=label.max_repeat * parent_repeat,
                                                      kind=label.kind,
                                                      choices=choices,
                                                      parent_label=parent_label))

        return ret

    return recurse(annotation_scheme.labels)


AnnotationModelLookupType = dict[str, list[AnnotationModel]]


def create_annotations_lookup(annotations: list[AnnotationModel]) -> AnnotationModelLookupType:
    annotations_map: AnnotationModelLookupType = {}
    for annotation in annotations:
        if annotation.key not in annotations_map:
            annotations_map[annotation.key] = []
        annotations_map[annotation.key].append(annotation)

    for key in annotations_map.keys():
        annotations_map[key] = sorted(annotations_map[key], key=lambda a: a.repeat)

    return annotations_map


def validate_annotated_assignment(annotation_scheme: AnnotationSchemeModel,
                                  annotations: list[AnnotationModel]) -> AssignmentStatus:
    if annotations is None or len(annotations) == 0:
        return AssignmentStatus.OPEN

    annotations_map = create_annotations_lookup(annotations)

    def recurse(labels: list[AnnotationSchemeLabel], repeat: int = 1, parent: str = None) -> AssignmentStatus:
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
                    status = AssignmentStatus.INVALID

        return status

    return recurse(annotation_scheme.labels)


def merge_scheme_and_annotations(annotation_scheme: AnnotationSchemeModel,
                                 annotations: list[AnnotationModel]) -> AnnotationSchemeModel:
    if annotations is None or len(annotations) == 0:
        return annotation_scheme

    annotations_map = create_annotations_lookup(annotations)

    def recurse(labels: list[AnnotationSchemeLabel], repeat: int = 1, parent: str = None) \
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
                        label_cpy = label.copy(deep=True)
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
    return label.annotation \
           and (label.annotation.value_int is not None
                or label.annotation.value_str is not None
                or label.annotation.value_bool is not None
                or label.annotation.value_float is not None)


def annotated_scheme_to_annotations(scheme: AnnotationSchemeModel) -> list[AnnotationModel]:
    ret = []

    def recurse(labels: list[AnnotationSchemeLabel], parent_id: UUID = None) -> None:
        for label in labels:
            if has_annotation(label):
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
