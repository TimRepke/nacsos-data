from enum import Enum
from uuid import UUID, uuid4
import logging
from nacsos_data.models.annotations import \
    AnnotationModel, AnnotationTaskModel, \
    AnnotationTaskLabelTypes, FlattenedAnnotationTaskLabel, \
    AssignmentStatus, AnnotationTaskLabel, AnnotationTaskLabelChoice

logger = logging.getLogger('nacsos_data.annotation.validation')


def flatten_annotation_task(annotation_task: AnnotationTaskModel) -> list[FlattenedAnnotationTaskLabel]:
    def recurse(labels, parent_label=None, parent_repeat=1):
        ret = []

        for label in labels:
            choices = None

            if label.choices is not None:
                choices = []
                for choice in label.choices:
                    choices.append(choice.value)
                    if choice.children is not None:
                        ret += recurse(choice.children,
                                       parent_label=label.key,
                                       parent_repeat=label.max_repeat)

            ret.append(FlattenedAnnotationTaskLabel(key=label.key,
                                                    required=label.required,
                                                    max_repeat=label.max_repeat,
                                                    implicit_max_repeat=label.max_repeat * parent_repeat,
                                                    kind=label.kind,
                                                    choices=choices,
                                                    parent_label=parent_label))

        return ret

    return recurse(annotation_task.labels)


def validate_annotated_assignment(annotation_task: AnnotationTaskModel,
                                  annotations: list[AnnotationModel]) -> AssignmentStatus:
    status, _ = merge_task_and_annotations(annotation_task, annotations)
    return status


def merge_task_and_annotations(annotation_task: AnnotationTaskModel,
                               annotations: list[AnnotationModel]) -> tuple[AssignmentStatus, AnnotationTaskModel]:
    if annotations is None or len(annotations) == 0:
        return AssignmentStatus.OPEN, annotation_task

    annotations_map = {}
    for annotation in annotations:
        if annotation.key not in annotations_map:
            annotations_map[annotation.key] = []
        annotations_map[annotation.key].append(annotation)

    for key in annotations_map.keys():
        annotations_map[key] = sorted(annotations_map[key], key=lambda a: a.repeat)

    def recurse(labels: list[AnnotationTaskLabel], repeat: int = 1, parent: str | UUID = None):
        ret = []
        status = AssignmentStatus.FULL

        for label in labels:
            if label.key not in annotations_map:
                ret.append(label)
                if label.required:
                    status = AssignmentStatus.PARTIAL
            else:
                cnt = 0
                for annotation in annotations_map[label.key]:
                    if annotation.parent == parent:
                        cnt += 1
                        label_cpy = label.copy(deep=True)
                        label_cpy.annotation = annotation

                        for ci, choice in enumerate(label_cpy.choices or []):
                            if choice.children is not None:
                                child_state, children = recurse(choice.children,
                                                                repeat=repeat + 1,
                                                                parent=annotation.annotation_id)
                                choice.children = children

                                if child_state != AssignmentStatus.FULL:
                                    status = child_state
                        ret.append(label_cpy)
                if label.required and cnt == 0:
                    status = AssignmentStatus.PARTIAL
                if cnt > label.max_repeat:
                    status = AssignmentStatus.INVALID

        return status, ret

    assignment_status, annotation_task.labels = recurse(annotation_task.labels)
    return assignment_status, annotation_task


def has_annotation(label: AnnotationTaskLabel) -> bool:
    return label.annotation \
           and (label.annotation.value_int is not None
                or label.annotation.value_str is not None
                or label.annotation.value_bool is not None
                or label.annotation.value_float is not None)


def annotated_task_to_annotations(task: AnnotationTaskModel) -> list[AnnotationModel]:
    ret = []

    def recurse(labels: list[AnnotationTaskLabel], parent_id: UUID = None):
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

    recurse(task.labels)

    return ret
