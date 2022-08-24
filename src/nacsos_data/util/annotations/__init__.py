from typing import Any

from nacsos_data.models.annotations import AnnotationSchemeModel, AnnotationSchemeLabel


def unravel_annotation_scheme_keys(scheme: AnnotationSchemeModel) -> list[str]:
    def recurse_label(label: AnnotationSchemeLabel | None, accu: list[str]) -> list[Any]:
        if label is None:
            return accu
        accu += [label.key]
        if label.choices is None:
            return accu

        return [recurse_label(child, accu)
                for choice in label.choices
                if choice is not None and choice.children is not None
                for child in choice.children]

    keys = [recurse_label(label, []) for label in scheme.labels]
    return [k for kk in keys for k in kk]
