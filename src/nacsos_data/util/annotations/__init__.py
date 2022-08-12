from nacsos_data.models.annotations import AnnotationSchemeModel, AnnotationSchemeLabel


def unravel_annotation_scheme_keys(scheme: AnnotationSchemeModel):
    def recurse_label(label: AnnotationSchemeLabel, accu: list[str]):
        if label is None:
            return accu
        accu += [label.key]
        if label.choices is None:
            return accu

        return [recurse_label(choice.child, accu) for choice in label.choices]

    keys = [recurse_label(label, []) for label in scheme.labels]
    return keys
