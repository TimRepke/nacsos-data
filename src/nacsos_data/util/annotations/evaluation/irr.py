import uuid
import logging
import itertools
import warnings
from typing import TypeAlias, Literal, TypeVar, Any

import numpy as np
from scipy.stats import pearsonr, kendalltau, spearmanr, ConstantInputWarning
from sklearn.exceptions import UndefinedMetricWarning
from sklearn.metrics import cohen_kappa_score, precision_recall_fscore_support

from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.crud.annotations import read_annotation_scheme_for_scope
from nacsos_data.db.crud.users import user_ids_to_names
from nacsos_data.db.engine import ensure_session_async
from nacsos_data.models.annotation_quality import AnnotationQualityModel
from nacsos_data.models.annotations import AnnotationSchemeModel, FlatLabel
from nacsos_data.util.annotations.label_transform import get_annotations, annotations_to_sequence, SortedAnnotationLabel
from nacsos_data.util.annotations.validation import labels_from_scheme
from nacsos_data.util.errors import NotFoundError

logger = logging.getLogger('nacsos_data.util.annotations.evaluation.irr')

AnnotationRaw: TypeAlias = int | list[int] | None
AnnotationsRaw: TypeAlias = list[int | None] | list[list[int] | None]
Annotation: TypeAlias = int | list[int]
Annotations: TypeAlias = list[int] | list[list[int]]


def get_value_raw(annotation: SortedAnnotationLabel | None, label: FlatLabel) -> AnnotationRaw:
    if annotation is None:
        return None
    if label.kind == 'bool':
        return int(annotation.value_bool)
    elif label.kind == 'int':
        return annotation.value_int
    elif label.kind == 'single':
        return annotation.value_int
    elif label.kind == 'multi':
        return annotation.multis
    else:
        return None


def get_overlap(annotations_base: AnnotationsRaw, annotations_target: AnnotationsRaw) \
        -> tuple[Annotations, Annotations]:
    base: Annotations = []  # type: ignore[assignment]
    target: Annotations = []  # type: ignore[assignment]
    for annotation_base, annotation_target in zip(annotations_base, annotations_target):
        if annotation_base is not None and annotation_target is not None:
            base.append(annotation_base)
            target.append(annotation_target)
    return base, target


def get_partial_overlap(annotations: dict[str, AnnotationsRaw], users: list[str] | None = None) \
        -> dict[str, list[AnnotationsRaw]]:
    if users is None:
        users = list(annotations.keys())

    annotations_filtered: dict[str, AnnotationsRaw] = {user: [] for user in users}
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
            return fix(cohen_kappa_score(base, target))  # type: ignore[no-any-return]  # FIXME
        except RuntimeWarning as e:
            logger.error(e)
            return None
        except UserWarning as e:
            logger.error(e)
            return None
        except Exception as e:
            logger.error(e)
            logger.error(base)
            logger.error(target)
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
                return fix(result.statistic), fix(result.pvalue)
            elif measure == 'kendall':
                result = kendalltau(base, target)
                return fix(result.statistic), fix(result.pvalue)
            elif measure == 'spearman':
                result = spearmanr(base, target)
                return fix(result.statistic), fix(result.pvalue)
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
        return 1.

    kappa: float = (p_mean - p_mean_exp) / (1 - p_mean_exp)
    return fix(kappa)


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
    return fix(result)


def compute_multi_overlap(base: list[list[int]], target: list[list[int]]) -> tuple[float, float, float]:
    overlaps = []
    for b, t in zip(base, target):
        bs = set(b)
        ts = set(t)
        overlaps.append(len(bs & ts) / len(bs | ts))
    arr = np.array(overlaps)
    return fix(np.mean(arr)), fix(np.median(arr)), fix(np.std(arr))


def compute_agreement(base: Annotations, target: Annotations) -> tuple[int, int, float]:
    num_agree = len([1 for b, t in zip(base, target) if b == t])
    num_disagree = len([1 for b, t in zip(base, target) if b != t])

    return num_agree, num_disagree, fix((num_agree / len(base)) * 100)


def compute_mean(metric: str, label_qualities: list[AnnotationQualityModel]) -> float | None:
    values = [getattr(lq, metric) for lq in label_qualities if getattr(lq, metric) is not None]
    if len(values) > 0:
        result: float = sum(values) / len(values)
        return fix(result)
    return None


def precision_recall_f1(base, target, average: str) -> tuple[float | None, float | None, float | None]:
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        try:
            p, r, f, _ = precision_recall_fscore_support(base, target, average=average)
            return fix(p), fix(r), fix(f)
        except UndefinedMetricWarning:
            return None, None, None


def fix(val: float | None) -> float | None:
    return float(val) if val is not None and not np.isnan(val) else None  # type: ignore[call-overload]


@ensure_session_async
async def compute_irr_scores(session: AsyncSession,
                             assignment_scope_id: str | uuid.UUID,
                             resolution_id: str | uuid.UUID | None = None,
                             project_id: str | uuid.UUID | None = None,
                             include_key: str = '-[include]-') -> list[AnnotationQualityModel]:
    scheme: AnnotationSchemeModel | None = await read_annotation_scheme_for_scope(
        assignment_scope_id=assignment_scope_id,
        session=session)
    if not scheme:
        raise NotFoundError(f'No annotation scheme for scope {assignment_scope_id}')
    labels = labels_from_scheme(scheme, ignore_hierarchy=False, ignore_repeat=False)

    source_ids = [assignment_scope_id]
    if resolution_id is not None:
        source_ids.append(resolution_id)

    user_map = await user_ids_to_names(session=session)
    annotations = await get_annotations(session=session, source_ids=source_ids)

    inclusions: list[int] | None = None
    if scheme.inclusion_rule:
        inclusions = annotations_to_sequence(inclusion_rule=scheme.inclusion_rule, annotations=annotations)
        labels.append(FlatLabel(path=[], repeat=1, path_key=f'{include_key}|1', name='Inclusion Rule', key=include_key,
                                required=True, max_repeat=1, kind='bool'))
    annotation_map = {}
    annotators = []
    item_order = []
    for ai, annotation in enumerate(annotations):
        item_id = str(annotation.item_id)
        user_key = str(annotation.user_id)
        if user_key in user_map:
            user_key = user_map[user_key]
        annotators.append(user_key)
        item_order.append(item_id)

        if item_id not in annotation_map:
            annotation_map[item_id] = {}
        if user_key not in annotation_map[item_id]:
            annotation_map[item_id][user_key] = {}

        annotation_map[item_id][user_key] = annotation.labels
        if inclusions:
            annotation_map[item_id][user_key][include_key] = SortedAnnotationLabel(value_bool=bool(inclusions[ai]),
                                                                                   values_bool=[bool(inclusions[ai])])

    annotators = list(set(annotators))
    item_order = list(dict.fromkeys(item_order).keys())

    logger.debug(annotators)

    qualities: list[AnnotationQualityModel] = []
    for label in labels:
        logger.debug(f'Computing IRR for label {label.path_key}')
        if label.kind == 'str' or label.kind == 'float':
            logger.info(f'Skipping label {label.path_key} for scope {assignment_scope_id} '
                        f'because "{label.kind}" is not supported!')
            continue

        label_qualities = []

        user_annotations_raw = {
            annotator: [
                get_value_raw(annotation_map[item_id].get(annotator, {}).get(label.key), label)
                for item_id in item_order
            ]
            for annotator in annotators
        }

        for ui, user_base in enumerate(annotators):
            for user_target in annotators[ui + 1:]:
                annotations_base = user_annotations_raw[user_base]
                annotations_target = user_annotations_raw[user_target]
                base, target = get_overlap(annotations_base, annotations_target)

                if len(base) == 0 or len(target) == 0 or len(base) != len(target):
                    logger.warning(f'There is no annotation overlap between '
                                   f'{user_base} and {user_target} for label {label.path_key}.')
                else:
                    num_agree, num_disagree, perc_agree = compute_agreement(base, target)
                    quality = AnnotationQualityModel(
                        assignment_scope_id=assignment_scope_id,
                        bot_annotation_metadata_id=resolution_id,
                        project_id=project_id,
                        user_base=user_base,
                        annotations_base=user_annotations_raw[user_base],
                        user_target=user_target,
                        annotations_target=user_annotations_raw[user_target],
                        label_key=label.key,
                        label_value=None,
                        num_items=len(item_order),
                        num_overlap=len(base),
                        num_agree=num_agree,
                        num_disagree=num_disagree,
                        perc_agree=perc_agree
                    )
                    if label.kind == 'multi':
                        quality.multi_overlap_mean, quality.multi_overlap_median, quality.multi_overlap_std = \
                            compute_multi_overlap(base, target)
                    else:
                        quality.precision, quality.recall, quality.f1 = precision_recall_f1(base, target,
                                                                                            average='macro')
                        quality.pearson, quality.pearson_p = compute_correlation(base, target, 'pearson')
                        quality.kendall, quality.kendall_p = compute_correlation(base, target, 'kendall')
                        quality.spearman, quality.spearman_p = compute_correlation(base, target, 'spearman')
                        quality.cohen = compute_cohen(base, target)
                        quality.fleiss = compute_fleiss(user_annotations_raw, method='fleiss',
                                                        users=[user_base, user_target])
                        quality.randolph = compute_fleiss(user_annotations_raw, method='randolph',
                                                          users=[user_base, user_target])
                        quality.krippendorff = compute_krippendorff(user_annotations_raw, 'nominal',
                                                                    users=[user_base, user_target])

                    qualities.append(quality)
                    label_qualities.append(quality)
                    logger.debug(f'nq: {len(qualities)}, nlq: {len(label_qualities)}')
                    if label.choices:
                        for choice in label.choices:
                            logger.debug(f'IRR for {label.path_key} = {choice.value} ({choice.name})')
                            if label.kind == 'multi':
                                base_ = [int(choice.value in bi) for bi in base]
                                target_ = [int(choice.value in ti) for ti in target]
                            else:
                                base_ = [int(int(bi) == int(choice.value)) for bi in base]
                                target_ = [int(int(ti) == int(choice.value)) for ti in target]

                            num_agree, num_disagree, perc_agree = compute_agreement(base_, target_)
                            pearson, pearson_p = compute_correlation(base_, target_, 'pearson')
                            kendall, kendall_p = compute_correlation(base_, target_, 'kendall')
                            spearman, spearman_p = compute_correlation(base_, target_, 'spearman')
                            cohen = compute_cohen(base_, target_)
                            precision, recall, f1 = precision_recall_f1(base_, target_, average='binary')

                            qualities.append(
                                AnnotationQualityModel(
                                    assignment_scope_id=assignment_scope_id,
                                    bot_annotation_metadata_id=resolution_id,
                                    project_id=project_id,
                                    user_base=user_base,
                                    annotations_base=user_annotations_raw[user_base],
                                    user_target=user_target,
                                    annotations_target=user_annotations_raw[user_target],
                                    label_key=label.key,
                                    label_value=int(choice.value),
                                    pearson=pearson,
                                    pearson_p=pearson_p,
                                    kendall=kendall,
                                    kendall_p=kendall_p,
                                    spearman=spearman,
                                    spearman_p=spearman_p,
                                    cohen=cohen,
                                    num_items=len(item_order),
                                    num_overlap=len(base_),
                                    num_agree=num_agree,
                                    num_disagree=num_disagree,
                                    perc_agree=perc_agree,
                                    precision=fix(precision),
                                    recall=fix(recall),
                                    f1=fix(f1)
                                )
                            )

        if len(label_qualities) > 0:
            if label.kind == 'multi':
                fleiss = None
                randolph = None
                krippendorff = None
            else:
                fleiss = compute_fleiss(user_annotations_raw, method='fleiss')
                randolph = compute_fleiss(user_annotations_raw, method='randolph')
                krippendorff = compute_krippendorff(user_annotations_raw, 'nominal')

            qualities.append(
                AnnotationQualityModel(
                    assignment_scope_id=assignment_scope_id,
                    bot_annotation_metadata_id=resolution_id,
                    project_id=project_id,
                    label_key=label.key,
                    label_value=None,
                    cohen=compute_mean('cohen', label_qualities),
                    fleiss=fix(fleiss),
                    randolph=fix(randolph),
                    krippendorff=fix(krippendorff),
                    pearson=compute_mean('pearson', label_qualities),
                    pearson_p=compute_mean('pearson_p', label_qualities),
                    kendall=compute_mean('kendall', label_qualities),
                    kendall_p=compute_mean('kendall_p', label_qualities),
                    spearman=compute_mean('spearman', label_qualities),
                    spearman_p=compute_mean('spearman_p', label_qualities),
                    precision=compute_mean('precision', label_qualities),
                    recall=compute_mean('recall', label_qualities),
                    f1=compute_mean('f1', label_qualities),
                    num_overlap=compute_mean('num_overlap', label_qualities),
                    num_items=compute_mean('num_items', label_qualities),
                    num_agree=compute_mean('num_agree', label_qualities),
                    num_disagree=compute_mean('num_disagree', label_qualities),
                    perc_agree=compute_mean('perc_agree', label_qualities),
                    multi_overlap_mean=compute_mean('multi_overlap_mean', label_qualities),
                    multi_overlap_median=compute_mean('multi_overlap_median', label_qualities),
                    multi_overlap_std=compute_mean('multi_overlap_std', label_qualities)
                )
            )

    return qualities
