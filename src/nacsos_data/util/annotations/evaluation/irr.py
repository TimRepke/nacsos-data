import uuid
import logging
import itertools
import warnings
from typing import TypeAlias, Literal, TypeVar, Any

import numpy as np
from scipy.stats import pearsonr, kendalltau, spearmanr, ConstantInputWarning
from sklearn.metrics import cohen_kappa_score

from sqlalchemy import select, distinct
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.engine import ensure_session_async
from nacsos_data.db.schemas import AssignmentScope, Assignment
from nacsos_data.models.annotation_quality import AnnotationQualityModel
from nacsos_data.models.annotations import AnnotationSchemeModel, FlatLabel, ItemAnnotation, FlatLabelChoice
from nacsos_data.models.bot_annotations import AssignmentMap, OrderingEntry, ResolutionMatrix, ResolutionCell, \
    ResolutionUserEntry
from nacsos_data.models.users import UserModel
from nacsos_data.util.annotations.resolve import get_annotation_matrix
from nacsos_data.util.errors import NotFoundError

logger = logging.getLogger('nacsos_data.util.annotations.evaluation.irr')

AnnotationRaw: TypeAlias = int | list[int] | str | float | bool | None
AnnotationsRaw: TypeAlias = (list[int | None]
                             | list[list[int] | None]
                             | list[str | None]
                             | list[bool | None]
                             | list[float | None])


def get_value_raw(annotation: ItemAnnotation | None, label: FlatLabel) -> AnnotationRaw:
    if annotation is None:
        return None
    if label.kind == 'bool':
        return annotation.value_bool
    elif label.kind == 'str':
        return annotation.value_str
    elif label.kind == 'float':
        return annotation.value_float
    elif label.kind == 'int':
        return annotation.value_int
    elif label.kind == 'single':
        return annotation.value_int
    elif label.kind == 'multi':
        return annotation.multi_int
    else:
        return None


def get_value(annotation: ItemAnnotation | None, label: FlatLabel) -> int | list[int] | None:
    if annotation is None:
        return None
    if label.kind == 'bool':
        return int(annotation.value_bool) if annotation.value_bool is not None else None
    elif label.kind == 'int':
        return annotation.value_int
    elif label.kind == 'single':
        return annotation.value_int
    elif label.kind == 'multi':
        return annotation.multi_int
    else:
        return None


def translate_multi(annotations: list[list[int] | None], choices: list[FlatLabelChoice]) -> list[int | None]:
    values = {choice.value: idx for idx, choice in enumerate(choices)}
    annotations_translated: list[int | None] = []
    for annotation in annotations:
        if annotation is None:
            annotations_translated.append(None)
        else:
            tmp = ['0'] * len(values)
            for choice in annotation:
                tmp[values[choice]] = '1'
            annotations_translated.append(int(''.join(reversed(tmp)), 2))
    return annotations_translated


def pluck_annotations(label: FlatLabel, user: UserModel,
                      item_order: list[OrderingEntry], annotation_map: ResolutionMatrix) \
        -> list[list[int] | None] | list[int | None]:
    annotations: list[list[int] | None] | list[int | None] = []  # type: ignore[assignment]
    for item in item_order:
        item_annotations: dict[str, ResolutionCell] = annotation_map.get(item.key, {})
        label_item_annotations: ResolutionCell | None = item_annotations.get(label.path_key)
        if label_item_annotations is not None:
            user_annotations: list[ResolutionUserEntry] | None = label_item_annotations.labels.get(str(user.user_id))
            # No annotation for this label for this item by this user
            if user_annotations is None or len(user_annotations) == 0:
                annotations.append(None)
            else:
                user_annotation = user_annotations[0].annotation
                user_value = get_value(user_annotation, label)
                annotations.append(user_value)  # type: ignore[arg-type]
        else:
            # No annotation for this label for this item
            annotations.append(None)

    return annotations


def get_overlap(annotations_base: list[int | None], annotations_target: list[int | None]) \
        -> tuple[list[int], list[int]]:
    base: list[int] = []  # type: ignore[assignment]
    target: list[int] = []  # type: ignore[assignment]
    for annotation_base, annotation_target in zip(annotations_base, annotations_target):
        if annotation_base is not None and annotation_target is not None:
            base.append(annotation_base)
            target.append(annotation_target)
    return base, target


def get_partial_overlap(annotations: dict[str, list[int | None]], users: list[str] | None = None) \
        -> dict[str, list[int | None]]:
    if users is None:
        users = list(annotations.keys())

    annotations_filtered: dict[str, list[int | None]] = {user: [] for user in users}
    for row in zip(*[annotations[user] for user in users]):
        if len([1 for v in row if v is not None]) > 0:
            for user, v in zip(users, row):
                annotations_filtered[user].append(v)
    return annotations_filtered


def compute_cohen(base: list[int], target: list[int]) -> float | None:
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        try:
            base, target = compress_annotations(base, target)
            return cohen_kappa_score(base, target)  # type: ignore[no-any-return]  # FIXME
        except RuntimeWarning:
            return None
        except Exception as e:

            print(base)
            print(target)
            raise e


def compress_annotations(base: list[int], target: list[int]) -> tuple[list[int], list[int]]:
    # "Compress" the labels that might be all over the place to 0, 1, 2, ...
    values = {v: i for i, v in enumerate(set(base + target))}
    base = [values[v] for v in base]
    target = [values[v] for v in target]
    return base, target


def compute_correlation(base: list[int], target: list[int],
                        measure: Literal['pearson', 'kendall', 'spearman']) -> tuple[float, float] | tuple[None, None]:
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        try:
            base, target = compress_annotations(base, target)
            if measure == 'pearson':
                result = pearsonr(base, target)
                return result.statistic, result.pvalue
            elif measure == 'kendall':
                result = kendalltau(base, target)
                return result.statistic, result.pvalue
            elif measure == 'spearman':
                result = spearmanr(base, target)
                return result.statistic, result.pvalue
        except ConstantInputWarning:
            return None, None
        except ValueError:
            return None, None


T = TypeVar('T')


def get_values(annotations: dict[str, list[T | None]],
               users: list[str] | None = None,
               include_none: bool = False) -> dict[T | None, int] | dict[T, int]:
    if users is None:
        users = list(annotations.keys())

    return {v: i for i, v in enumerate(set([annotation
                                            for user in users
                                            for annotation in annotations[user]
                                            if include_none or annotation is not None]))}


def get_coincidence_matrix(annotations: dict[str, list[int | None]], users: list[str] | None = None) \
        -> tuple[dict[int, int], np.ndarray[Any, np.dtype[np.float64]]]:
    if users is None:
        users = list(annotations.keys())

    values: dict[int, int] = get_values(annotations=annotations, users=users)  # type: ignore[assignment]
    coincidence_matrix = np.zeros((len(values), len(values)))
    for row in zip(*[annotations[user] for user in users]):
        row_values = [values[r] for r in row if r is not None]
        num_annotations = len(row_values)
        perms = itertools.permutations(row_values, 2)
        for perm in perms:
            i, j = perm[0], perm[1]
            coincidence_matrix[i][j] += 1 / (num_annotations - 1)

    return values, coincidence_matrix


def compute_fleiss(annotations: dict[str, list[int | None]],
                   method: Literal['fleiss', 'randolph'],
                   users: list[str] | None = None) -> float | None:
    """
    Fleiss’ and Randolph’s kappa multi-rater agreement measure

    Heavily inspired by:
    https://www.statsmodels.org/stable/generated/statsmodels.stats.inter_rater.fleiss_kappa.html

    Interrater agreement measures like Fleiss’s kappa measure agreement relative to chance agreement.
    Different authors have proposed ways of defining these chance agreements. Fleiss’ is based on the marginal sample
    distribution of categories, while Randolph uses a uniform distribution of categories as benchmark. Warrens (2010)
    showed that Randolph’s kappa is always larger or equal to Fleiss’ kappa. Under some commonly observed condition,
    Fleiss’ and Randolph’s kappa provide lower and upper bounds for two similar kappa_like measures
    by Light (1971) and Hubert (1977).

    :param annotations:
    :param method:
    :param users:
    :return:
    """
    if users is None:
        users = list(annotations.keys())

    # Drop unwanted users and items without any annotation from the list
    annotations = get_partial_overlap(annotations, users)

    n_items = len(annotations[users[0]])
    values = get_values(annotations, users)

    if n_items == 0 or len(values) == 0:
        return None

    table = np.zeros((n_items, len(values)))
    for user in users:
        for row, annotation in enumerate(annotations[user]):
            if annotation is not None:
                table[row][values[annotation]] += 1

    n_sub, n_cat = table.shape
    n_total = table.sum()
    n_rater = table.sum(1)
    n_rat = n_rater.max()

    # not fully ranked
    if n_rat == 1:  # FIXME used to be: n_total != n_sub * n_rat or n_rat == 1
        return None

    # marginal frequency  of categories
    p_cat = table.sum(0) / n_total

    table2 = table * table
    p_rat = (table2.sum(1) - n_rat) / (n_rat * (n_rat - 1.))
    p_mean = p_rat.mean()

    if method == 'fleiss':
        p_mean_exp = (p_cat * p_cat).sum()
    elif method == 'randolph':
        p_mean_exp = 1 / n_cat
    else:
        p_mean_exp = 0  # type: ignore[unreachable]

    if p_mean_exp == 1:
        return 1

    kappa: float = (p_mean - p_mean_exp) / (1 - p_mean_exp)
    return kappa


def compute_krippendorff(annotations: dict[str, list[int | None]],
                         data_type: Literal['nominal', 'ordinal', 'interval', 'ratio'],
                         users: list[str] | None = None) -> float | None:
    """
    Compute Krippendorff's alpha statistic between annotations agreements

    :param annotations:
    :param data_type:
    :param users:
    :return:
    """
    if users is None:
        users = list(annotations.keys())

    # Drop unwanted users and items without any annotation from the list
    annotations = get_partial_overlap(annotations, users)

    values, coincidence_matrix = get_coincidence_matrix(annotations, users=users)
    coincidence_matrix_sum = coincidence_matrix.sum(axis=0)
    values_inv = {v: k for k, v in values.items()}

    def delta_nominal(v1: int, v2: int) -> float:
        if v1 == v2:
            return 0.0
        else:
            return 1.0

    def delta_ordinal(v1: int, v2: int) -> float:
        val = 0
        for g in range(v1, v2 + 1):
            element1 = coincidence_matrix_sum[g]
            val += element1

        element2 = (coincidence_matrix_sum[v1] + coincidence_matrix_sum[v2]) / 2.
        val = val - element2

        return val ** 2

    def delta_interval(v1: float | int, v2: float | int) -> float:
        return (v1 - v2) ** 2

    def delta_ratio(v1: float | int, v2: float | int) -> float:
        return ((v1 - v2) / (v1 + v2)) ** 2

    def disagreement(obs_or_exp: Literal['observed', 'expected']) -> float:
        result = 0.
        for vi_1 in range(1, len(values)):
            for vi_2 in range(vi_1):
                v1 = values_inv[vi_1]
                v2 = values_inv[vi_2]
                if data_type == 'nominal':
                    delta = delta_nominal(v1, v2)
                elif data_type == 'ordinal':
                    delta = delta_ordinal(v1, v2)
                elif data_type == 'interval':
                    delta = delta_interval(v1, v2)
                elif data_type == 'ratio':
                    delta = delta_ratio(v1, v2)
                else:
                    raise AssertionError(f'Unknown data_type={data_type}')

                if obs_or_exp == 'observed':
                    result += (coincidence_matrix[vi_1][vi_2] * delta)
                else:
                    result += (coincidence_matrix_sum[vi_1] * coincidence_matrix_sum[vi_2] * delta)
        return result

    observed_disagreement = disagreement(obs_or_exp='observed')
    expected_disagreement = disagreement(obs_or_exp='expected')

    if expected_disagreement == 0:
        return 1.

    n_total = sum(coincidence_matrix_sum)

    result: float = 1. - (n_total - 1.) * (observed_disagreement / expected_disagreement)
    return result


def compute_mean(metric: str, label_qualities: list[AnnotationQualityModel]) -> float | None:
    values = [getattr(lq, metric) for lq in label_qualities if getattr(lq, metric) is not None]
    if len(values) > 0:
        result: float = sum(values) / len(values)
        return result
    return None


@ensure_session_async
async def compute_irr_scores(session: AsyncSession,
                             assignment_scope_id: str | uuid.UUID,
                             project_id: str | uuid.UUID | None = None) -> list[AnnotationQualityModel]:
    scheme_id = (await session.scalar(select(AssignmentScope.annotation_scheme_id)
                                      .where(AssignmentScope.assignment_scope_id == assignment_scope_id)))

    if scheme_id is None:
        raise NotFoundError(f'No assignment scope with id {assignment_scope_id}')

    user_ids: list[uuid.UUID] = (await session.execute(  # type: ignore[assignment]
        select(distinct(Assignment.user_id))
        .where(Assignment.assignment_scope_id == assignment_scope_id)
    )).scalars().all()

    scheme: AnnotationSchemeModel
    labels: list[FlatLabel]
    annotators: list[UserModel]
    assignments: AssignmentMap
    annotations: list[ItemAnnotation]
    item_order: list[OrderingEntry]
    annotation_map: ResolutionMatrix
    scheme, labels, annotators, assignments, annotations, item_order, annotation_map = await get_annotation_matrix(
        assignment_scope_id=str(assignment_scope_id),
        ignore_hierarchy=False,
        ignore_repeat=False,
        session=session
    )

    qualities: list[AnnotationQualityModel] = []

    for label in labels:
        if label.kind == 'str' or label.kind == 'float':
            logger.info(f'Skipping label {label.path_key} for scope {assignment_scope_id} '
                        f'because "{label.kind}" is not supported!')
            continue

        user_annotations_raw = {
            str(annotator.user_id): pluck_annotations(label=label, user=annotator,
                                                      item_order=item_order, annotation_map=annotation_map)
            for annotator in annotators
        }

        user_annotations: dict[str, list[int | None]]
        if label.kind == 'multi':
            if label.choices is None:
                raise AssertionError('Choices for multi label are missing; this should never happen!')
            user_annotations = {
                k: translate_multi(v, choices=label.choices)  # type: ignore[arg-type]
                for k, v in user_annotations_raw.items()
            }
        else:
            user_annotations = user_annotations_raw  # type: ignore[assignment]

        label_qualities = []

        for ai, annotator_base in enumerate(annotators):
            user_base = str(annotator_base.user_id)
            for annotator_target in annotators[ai + 1:]:
                user_target = str(annotator_target.user_id)
                annotations_base = user_annotations[user_base]
                annotations_target = user_annotations[user_target]
                base, target = get_overlap(annotations_base, annotations_target)

                if len(base) == 0 or len(target) == 0:
                    logger.warning(f'There is no annotation overlap between '
                                   f'{annotator_base.username} and {annotator_target.username} '
                                   f'for label {label.path_key}.')
                else:
                    pearson, pearson_p = compute_correlation(base, target, 'pearson')
                    kendall, kendall_p = compute_correlation(base, target, 'kendall')
                    spearman, spearman_p = compute_correlation(base, target, 'spearman')
                    cohen = compute_cohen(base, target)
                    fleiss = compute_fleiss(user_annotations, method='fleiss', users=[user_base, user_target])
                    randolph = compute_fleiss(user_annotations, method='randolph', users=[user_base, user_target])
                    krippendorff = compute_krippendorff(user_annotations, 'nominal',
                                                        users=[user_base, user_target])

                    quality = AnnotationQualityModel(
                        assignment_scope_id=assignment_scope_id,
                        project_id=project_id,
                        user_base=user_base,
                        annotations_base=user_annotations_raw[user_base],
                        user_target=user_target,
                        annotations_target=user_annotations_raw[user_target],
                        label_path_key=label.path_key,
                        label_path=label.path,
                        label_key=label.key,
                        cohen=cohen if cohen is not None and not np.isnan(cohen) else None,
                        fleiss=fleiss if fleiss is not None and not np.isnan(fleiss) else None,
                        randolph=randolph if randolph is not None and not np.isnan(randolph) else None,
                        krippendorff=krippendorff if krippendorff is not None and not np.isnan(krippendorff) else None,
                        pearson=pearson if pearson is not None and not np.isnan(pearson) else None,
                        pearson_p=pearson_p if pearson_p is not None and not np.isnan(pearson_p) else None,
                        kendall=kendall if kendall is not None and not np.isnan(kendall) else None,
                        kendall_p=kendall_p if kendall_p is not None and not np.isnan(kendall_p) else None,
                        spearman=spearman if spearman is not None and not np.isnan(spearman) else None,
                        spearman_p=spearman_p if spearman_p is not None and not np.isnan(spearman_p) else None,
                        num_items=len(item_order),
                        num_overlap=len(base),
                        num_agree=len([1 for b, t in zip(base, target) if b == t]),
                        num_disagree=len([1 for b, t in zip(base, target) if b != t])
                    )
                    qualities.append(quality)
                    label_qualities.append(quality)

        if len(label_qualities) > 0:
            fleiss = compute_fleiss(user_annotations, method='fleiss')
            randolph = compute_fleiss(user_annotations, method='randolph')
            krippendorff = compute_krippendorff(user_annotations, 'nominal')

            qualities.append(
                AnnotationQualityModel(
                    assignment_scope_id=assignment_scope_id,
                    project_id=project_id,
                    label_path_key=label.path_key,
                    label_path=label.path,
                    label_key=label.key,
                    cohen=compute_mean('cohen', label_qualities),
                    fleiss=fleiss if fleiss is not None and not np.isnan(fleiss) else None,
                    randolph=randolph if randolph is not None and not np.isnan(randolph) else None,
                    krippendorff=krippendorff if krippendorff is not None and not np.isnan(krippendorff) else None,
                    pearson=compute_mean('pearson', label_qualities),
                    pearson_p=compute_mean('pearson_p', label_qualities),
                    kendall=compute_mean('kendall', label_qualities),
                    kendall_p=compute_mean('kendall_p', label_qualities),
                    spearman=compute_mean('spearman', label_qualities),
                    spearman_p=compute_mean('spearman_p', label_qualities)
                )
            )

    return qualities
