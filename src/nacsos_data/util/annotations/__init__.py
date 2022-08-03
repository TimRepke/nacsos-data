from nacsos_data.models.annotations import AnnotationTaskModel, AnnotationTaskLabel


def unravel_annotation_task_keys(task: AnnotationTaskModel):
    def recurse_label(label: AnnotationTaskLabel, accu: list[str]):
        if label is None:
            return accu
        accu += [label.key]
        if label.choices is None:
            return accu

        return [recurse_label(choice.child, accu) for choice in label.choices]

    keys = [recurse_label(label, []) for label in task.labels]
    return keys
