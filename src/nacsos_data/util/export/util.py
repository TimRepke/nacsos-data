import uuid
import enum
import logging
import json
import sqlalchemy as sa

from typing import Type, Any
from pydantic import BaseModel
from sqlalchemy.dialects import postgresql as psa
from datetime import datetime

from nacsos_data.db.schemas import AcademicItem, TwitterItem, LexisNexisItem
from nacsos_data.db.schemas.annotations import Annotation, Assignment
from nacsos_data.db.schemas.bot_annotations import BotAnnotation
from nacsos_data.models.annotations import AnnotationSchemeModel
from nacsos_data.util.annotations.validation import flatten_annotation_scheme

logger = logging.getLogger('nacsos_data.util.annotations.export')

F2CRetType = (
    tuple[Type[AcademicItem] | Type[TwitterItem] | Type[LexisNexisItem], list[Type[sa.Column]]]  # type: ignore[type-arg]
    | tuple[None, None]
)


class LabelOptions(BaseModel):
    key: str
    options_int: list[int] | None = None
    options_bool: list[bool] | None = None
    options_multi: list[int] | None = None
    strings: bool | None = None


def scheme_to_label_options(scheme: AnnotationSchemeModel) -> dict[str, LabelOptions]:
    flat_scheme = flatten_annotation_scheme(scheme)
    return {
        label.key: LabelOptions(
            key=label.key,
            options_int=[choice.value for choice in label.choices] if label.choices and label.kind == 'single' else None,
            options_multi=[choice.value for choice in label.choices] if label.choices and label.kind == 'multi' else None,
            options_bool=[True, False] if label.kind == 'bool' else None,
            strings=True if label.kind == 'str' else None,
        )
        for label in flat_scheme.labels
    }


def encode_excel(o: Any) -> Any:
    # Translate datetime into a string
    if isinstance(o, datetime):
        return o.strftime('%Y-%m-%dT%H:%M:%S')

    # Translate UUID to str
    if isinstance(o, uuid.UUID):
        return str(o)

    # Translate Enum to str
    if isinstance(o, enum.Enum):
        return o.value

    if isinstance(o, list) or isinstance(o, dict):
        return json.dumps(o)

    return o


def _bool_label_columns(key: str, repeat: int | None, cte: sa.CTE) -> list[sa.Label]:  # type: ignore[type-arg]
    conditions = [cte.c.key == key]
    label = lambda x: f'{key}|{x}'  # noqa: E731
    if repeat is not None:
        conditions.append(cte.c.repeat == repeat)
        label = lambda x: f'{key}({repeat})|{x}'  # noqa: E731
    return [
        sa.case((sa.func.count().filter(sa.and_(*conditions)) > 0, sa.func.max(sa.case((sa.and_(cte.c.value_bool == vb, *conditions), 1), else_=0)))).label(
            label(vs),  # type: ignore[no-untyped-call]
        )
        for vs, vb in [('0', False), ('1', True)]
    ]


def _single_label_columns(key: str, repeat: int | None, values: list[int], cte: sa.CTE) -> list[sa.Label]:  # type: ignore[type-arg]
    conditions = [cte.c.key == key]
    label = lambda x: f'{key}|{x}'  # noqa: E731
    if repeat is not None:
        conditions.append(cte.c.repeat == repeat)
        label = lambda x: f'{key}({repeat})|{x}'  # noqa: E731
    return [
        sa.case((sa.func.count().filter(sa.and_(*conditions)) > 0, sa.func.max(sa.case((sa.and_(cte.c.value_int == v, *conditions), 1), else_=0)))).label(
            label(v),  # type: ignore[no-untyped-call]
        )
        for v in values
    ]


def _multi_label_columns(key: str, repeat: int | None, values: list[int], cte: sa.CTE) -> list[sa.Label]:  # type: ignore[type-arg]
    conditions = [cte.c.key == key]
    label = lambda x: f'{key}|{x}'  # noqa: E731
    if repeat is not None:
        conditions.append(cte.c.repeat == repeat)
        label = lambda x: f'{key}({repeat})|{x}'  # noqa: E731
    return [
        sa.case(
            (sa.func.count().filter(sa.and_(*conditions)) > 0, sa.func.max(sa.case((sa.and_(sa.any_(cte.c.multi_int) == v, *conditions), 1), else_=0))),
        ).label(label(v))  # type: ignore[no-untyped-call]
        for v in values
    ]


def _str_label_columns(key: str, repeat: int | None, cte: sa.CTE) -> list[sa.Label]:  # type: ignore[type-arg]
    if repeat is None:
        condition = cte.c.key == key
        label = key
    else:
        condition = sa.and_(cte.c.key == key, cte.c.repeat == repeat)
        label = f'{key}({repeat})'
    # string_agg(value_str, ' || ') filter ( where key='com' )
    return [sa.func.aggregate_strings(cte.c.value_str, ' || ').filter(condition).label(label)]


def _get_label_selects(labels: dict[str, LabelOptions], repeats: list[int] | None, cte: sa.CTE) -> list[sa.Label]:  # type: ignore[type-arg]
    # FIXME: we are ignoring `repeat` for child labels for now, hence, exports might be inconsistent for ranked labels
    selects = []
    for label in labels.values():
        for repeat in repeats or [None]:  # type: ignore[list-item]
            if label.options_int:
                selects += _single_label_columns(label.key, repeat=repeat, cte=cte, values=label.options_int)
            elif label.options_bool:
                selects += _bool_label_columns(label.key, repeat=repeat, cte=cte)
            elif label.options_multi:
                selects += _multi_label_columns(label.key, repeat=repeat, cte=cte, values=label.options_multi)
            elif label.strings:
                selects += _str_label_columns(label.key, repeat=repeat, cte=cte)
            else:
                pass
                # raise RuntimeError('Invalid state')

    return selects


def _labels_subquery(  # noqa: C901
    bot_annotation_metadata_ids: list[str] | list[uuid.UUID] | None,
    assignment_scope_ids: list[str] | list[uuid.UUID] | None,
    user_ids: list[str] | list[uuid.UUID] | None,
    labels: dict[str, LabelOptions] | None,
    ignore_repeat: bool,
) -> sa.CTE:
    def _label_filter(Schema: Type[Annotation] | Type[BotAnnotation], label: LabelOptions) -> sa.ColumnElement[bool] | None:
        if label.options_int:
            return sa.and_(Schema.key == label.key, Schema.value_int.in_(label.options_int))
        if label.options_bool:
            return sa.and_(Schema.key == label.key, Schema.value_bool.in_(label.options_bool))
        if label.options_multi:
            return sa.and_(Schema.key == label.key, Schema.multi_int.overlap(label.options_multi))
        if label.strings:
            return Schema.key == label.key

        return None

    sub_queries = []
    if assignment_scope_ids is not None:
        where = [Assignment.assignment_scope_id.in_(assignment_scope_ids)]
        if user_ids is not None and len(user_ids) > 0:
            where.append(Assignment.user_id.in_(user_ids))
        if labels is not None:
            ors = [_label_filter(Annotation, label_) for label_ in labels.values()]
            ors = [o for o in ors if o is not None]
            if ors is not None and len(ors) > 0:
                where.append(sa.or_(*ors))  # type: ignore[arg-type]

        sub_queries.append(
            sa.select(
                Assignment.item_id,
                Assignment.user_id,
                Annotation.annotation_id.label('label_id'),
                Annotation.parent,
                Annotation.key,
                Annotation.repeat if not ignore_repeat else sa.literal(1, type_=sa.Integer).label('repeat'),
                Annotation.value_int,
                Annotation.value_bool,
                Annotation.value_str,
                Annotation.multi_int,
            )
            .join(Annotation, Annotation.assignment_id == Assignment.assignment_id, isouter=True)
            .where(*where),
        )

    if bot_annotation_metadata_ids is not None:
        where = [BotAnnotation.bot_annotation_metadata_id.in_(bot_annotation_metadata_ids)]
        if labels is not None:
            ors = [_label_filter(BotAnnotation, label_) for label_ in labels.values()]
            ors = [o for o in ors if o is not None]
            if len(ors) > 0:
                where.append(sa.or_(*ors))  # type: ignore[arg-type]

        sub_queries.append(
            sa.select(
                BotAnnotation.item_id,
                sa.literal(None, type_=psa.UUID).label('user_id'),
                BotAnnotation.bot_annotation_id.label('label_id'),
                BotAnnotation.parent,
                BotAnnotation.key,
                BotAnnotation.repeat if not ignore_repeat else sa.literal(1, type_=sa.Integer).label('repeat'),
                BotAnnotation.value_int,
                BotAnnotation.value_bool,
                BotAnnotation.value_str,
                BotAnnotation.multi_int,
            ).where(*where),
        )

    if len(sub_queries) > 1:
        return sa.union(*sub_queries).cte()
    if len(sub_queries) == 1:
        return sub_queries[0].cte()

    raise AssertionError('You need at least on subquery for labels!')
