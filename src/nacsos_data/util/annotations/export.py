import uuid
import logging
from collections import defaultdict
from typing import Type, TYPE_CHECKING, Generator

from pydantic import BaseModel
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psa
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.connection import DatabaseEngineAsync
from nacsos_data.db.schemas.annotations import Annotation, Assignment, AssignmentScope, AnnotationScheme
from nacsos_data.db.schemas.bot_annotations import BotAnnotation, BotAnnotationMetaData
from .. import get_attr, anding

from ..errors import NotFoundError
from ..nql import NQLQuery
from ...db.engine import ensure_session_async, DBSession
from ...db.schemas import User, ProjectPermissions, Project, AcademicItem, TwitterItem, LexisNexisItem
from ...models.nql import NQLFilter

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger('nacsos_data.util.annotations.export')


class LabelOptions(BaseModel):
    key: str
    options_int: list[int] | None = None
    options_bool: list[bool] | None = None
    options_multi: list[int] | None = None
    strings: bool | None = None


def _bool_label_columns(key: str, repeat: int | None, cte: sa.CTE) \
        -> list[sa.Label]:  # type: ignore[type-arg]
    conditions = [cte.c.key == key]
    label = lambda x: f'{key}|{x}'  # noqa: E731
    if repeat is not None:
        conditions.append(cte.c.repeat == repeat)
        label = lambda x: f'{key}({repeat})|{x}'  # noqa: E731
    return [
        sa.case((sa.func.count().filter(sa.and_(*conditions)) > 0,
                 sa.func.max(sa.case((sa.and_(cte.c.value_bool == vb, *conditions), 1), else_=0))))
        .label(label(vs))
        for vs, vb in [('0', False), ('1', True)]
    ]


def _single_label_columns(key: str, repeat: int | None, values: list[int], cte: sa.CTE) \
        -> list[sa.Label]:  # type: ignore[type-arg]
    conditions = [cte.c.key == key]
    label = lambda x: f'{key}|{x}'  # noqa: E731
    if repeat is not None:
        conditions.append(cte.c.repeat == repeat)
        label = lambda x: f'{key}({repeat})|{x}'  # noqa: E731
    return [
        sa.case((sa.func.count().filter(sa.and_(*conditions)) > 0,
                 sa.func.max(sa.case((sa.and_(cte.c.value_int == v, *conditions), 1), else_=0))))
        .label(label(v))
        for v in values
    ]


def _multi_label_columns(key: str, repeat: int | None, values: list[int], cte: sa.CTE) \
        -> list[sa.Label]:  # type: ignore[type-arg]
    conditions = [cte.c.key == key]
    label = lambda x: f'{key}|{x}'  # noqa: E731
    if repeat is not None:
        conditions.append(cte.c.repeat == repeat)
        label = lambda x: f'{key}({repeat})|{x}'  # noqa: E731
    return [
        sa.case((sa.func.count().filter(sa.and_(*conditions)) > 0,
                 sa.func.max(sa.case((sa.and_(sa.any_(cte.c.multi_int) == v, *conditions), 1), else_=0))))
        .label(label(v))
        for v in values
    ]


def _get_label_selects(labels: dict[str, LabelOptions],
                       repeats: list[int] | None,
                       cte: sa.CTE) -> list[sa.Label]:  # type: ignore[type-arg]
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
            else:
                pass
                # raise RuntimeError('Invalid state')

    return selects


def _labels_subquery(bot_annotation_metadata_ids: list[str] | list[uuid.UUID] | None,
                     assignment_scope_ids: list[str] | list[uuid.UUID] | None,
                     user_ids: list[str] | list[uuid.UUID] | None,
                     labels: dict[str, LabelOptions] | None,
                     ignore_repeat: bool) -> sa.CTE:
    def _label_filter(Schema: Type[Annotation] | Type[BotAnnotation],
                      label: LabelOptions) -> sa.ColumnElement[bool] | None:  # type: ignore[type-arg]
        if label.options_int:
            return sa.and_(Schema.key == label.key,
                           Schema.value_int.in_(label.options_int))
        if label.options_bool:
            return sa.and_(Schema.key == label.key,
                           Schema.value_bool.in_(label.options_bool))
        if label.options_multi:
            return sa.and_(Schema.key == label.key,
                           Schema.multi_int.overlap(label.options_multi))
        if label.strings:
            return Schema.key == label.key  # type: ignore[no-any-return]

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
            sa.select(Assignment.item_id,
                      Assignment.user_id,
                      Annotation.annotation_id.label('label_id'),
                      Annotation.parent,
                      Annotation.key,
                      Annotation.repeat if not ignore_repeat else sa.literal(1, type_=sa.Integer).label('repeat'),
                      Annotation.value_int,
                      Annotation.value_bool,
                      Annotation.value_str,
                      Annotation.multi_int)
            .join(Annotation, Annotation.assignment_id == Assignment.assignment_id, isouter=True)
            .where(*where)
        )

    if bot_annotation_metadata_ids is not None:
        where = [BotAnnotation.bot_annotation_metadata_id.in_(bot_annotation_metadata_ids)]
        if labels is not None:
            ors = [_label_filter(BotAnnotation, label_) for label_ in labels.values()]
            ors = [o for o in ors if o is not None]
            if len(ors) > 0:
                where.append(sa.or_(*ors))  # type: ignore[arg-type]

        sub_queries.append(
            sa.select(BotAnnotation.item_id,
                      sa.literal(None, type_=psa.UUID).label('user_id'),
                      BotAnnotation.bot_annotation_id.label('label_id'),
                      BotAnnotation.parent,
                      BotAnnotation.key,
                      BotAnnotation.repeat if not ignore_repeat else sa.literal(1, type_=sa.Integer).label('repeat'),
                      BotAnnotation.value_int,
                      BotAnnotation.value_bool,
                      BotAnnotation.value_str,
                      BotAnnotation.multi_int)
            .where(*where)
        )

    if len(sub_queries) > 1:
        return sa.union(*sub_queries).cte()
    if len(sub_queries) == 1:
        return sub_queries[0].cte()

    raise AssertionError('You need at least on subquery for labels!')


async def get_project_bot_scopes(project_id: str | uuid.UUID, db_engine: DatabaseEngineAsync) -> list[dict[str, str]]:
    session: AsyncSession
    async with db_engine.session() as session:
        stmt = sa.select(BotAnnotationMetaData.bot_annotation_metadata_id.cast(type_=sa.String).label('id'),
                         BotAnnotationMetaData.name) \
            .where(BotAnnotationMetaData.project_id == project_id) \
            .order_by(BotAnnotationMetaData.time_created)
        return [dict(r) for r in (await session.execute(stmt)).mappings().all()]


async def get_project_scopes(project_id: str | uuid.UUID, db_engine: DatabaseEngineAsync) -> list[dict[str, str]]:
    session: AsyncSession
    async with db_engine.session() as session:
        stmt = sa.select(AssignmentScope.assignment_scope_id.cast(type_=sa.String).label('id'),
                         AssignmentScope.name,
                         AnnotationScheme.annotation_scheme_id.cast(type_=sa.String).label('scheme_id'),
                         AnnotationScheme.name.label('scheme_name')) \
            .join(AnnotationScheme, AnnotationScheme.annotation_scheme_id == AssignmentScope.annotation_scheme_id) \
            .where(AnnotationScheme.project_id == project_id) \
            .order_by(AssignmentScope.time_created)
        return [dict(r) for r in (await session.execute(stmt)).mappings().all()]


async def get_project_users(project_id: str | uuid.UUID, db_engine: DatabaseEngineAsync) -> list[dict[str, str]]:
    session: AsyncSession
    async with db_engine.session() as session:
        stmt = sa.select(User.user_id.cast(type_=sa.String).label('id'),
                         User.username.label('name')) \
            .join(ProjectPermissions, ProjectPermissions.user_id == User.user_id) \
            .where(ProjectPermissions.project_id == project_id) \
            .order_by(User.username)
        return [dict(r) for r in (await session.execute(stmt)).mappings().all()]


async def get_labels(stmt_labels: sa.CTE, db_engine: DatabaseEngineAsync) -> dict[str, LabelOptions]:
    stmt_labels_ = sa.union(
        sa.select(stmt_labels.c.key, stmt_labels.c.value_int, stmt_labels.c.value_bool, stmt_labels.c.value_str,
                  sa.func.unnest(stmt_labels.c.multi_int).label('multis')),
        sa.select(stmt_labels.c.key, stmt_labels.c.value_int, stmt_labels.c.value_bool, stmt_labels.c.value_str,
                  sa.literal(None, type_=sa.Integer).label('multis')),
    ).subquery()

    stmt_options = sa.select(
        stmt_labels_.c.key,
        sa.func.array_agg(sa.distinct(stmt_labels_.c.value_int))
        .filter(stmt_labels_.c.value_int.isnot(None)).label('options_int'),
        sa.func.array_agg(sa.distinct(stmt_labels_.c.value_bool))
        .filter(stmt_labels_.c.value_bool.isnot(None)).label('options_bool'),
        sa.func.array_agg(sa.distinct(stmt_labels_.c.multis))
        .filter(stmt_labels_.c.multis.isnot(None)).label('options_multi'),
        (sa.func.count().filter(stmt_labels_.c.value_str.isnot(None)) > 0).label('strings')
    ) \
        .where(stmt_labels_.c.key.isnot(None)) \
        .group_by(stmt_labels_.c.key) \
        .order_by(stmt_labels_.c.key)

    session: AsyncSession
    async with db_engine.session() as session:
        result = (await session.execute(stmt_options)).mappings().all()

        # construct a lookup map of key->options/values/choices
        return {row['key']: LabelOptions.model_validate(row) for row in result}


async def get_project_labels(project_id: str | uuid.UUID, db_engine: DatabaseEngineAsync) -> dict[str, LabelOptions]:
    bot_scopes = await get_project_bot_scopes(project_id=project_id, db_engine=db_engine)
    scopes = await get_project_scopes(project_id=project_id, db_engine=db_engine)
    users = await get_project_users(project_id=project_id, db_engine=db_engine)

    bot_annotation_metadata_ids = [r['id'] for r in bot_scopes]
    assignment_scope_ids = [r['id'] for r in scopes]
    user_ids = [r['id'] for r in users]

    stmt_labels = _labels_subquery(bot_annotation_metadata_ids=bot_annotation_metadata_ids,
                                   assignment_scope_ids=assignment_scope_ids,
                                   user_ids=user_ids,
                                   labels=None,
                                   ignore_repeat=True)

    return await get_labels(stmt_labels=stmt_labels, db_engine=db_engine)


F2CRetType = (tuple[Type[AcademicItem]
                    | Type[TwitterItem]
                    | Type[LexisNexisItem], list[Type[sa.Column]]]  # type: ignore[type-arg]
              | tuple[None, None])


@ensure_session_async
async def prepare_export_table(session: DBSession | AsyncSession,
                               nql_filter: NQLFilter | None,
                               bot_annotation_metadata_ids: list[str] | list[uuid.UUID] | None,
                               assignment_scope_ids: list[str] | list[uuid.UUID] | None,
                               user_ids: list[str] | list[uuid.UUID] | None,
                               project_id: str | uuid.UUID,
                               labels: list[LabelOptions],
                               ignore_hierarchy: bool,
                               ignore_repeat: bool) -> list[dict[str, bool | int | str | None]]:
    project_type = await session.scalar(sa.select(Project.type).where(Project.project_id == project_id))

    if project_type is None:
        raise NotFoundError(f'No project with id={project_id}!')

    nql_query = NQLQuery(query=nql_filter,
                         project_id=str(project_id),
                         project_type=project_type)

    labels_map = {lab.key: lab for lab in labels}
    stmt_labels_base = _labels_subquery(bot_annotation_metadata_ids=bot_annotation_metadata_ids,
                                        assignment_scope_ids=assignment_scope_ids,
                                        user_ids=user_ids,
                                        labels=labels_map,
                                        ignore_repeat=ignore_repeat)

    if ignore_hierarchy:
        # Prepare the CASE expressions to spread label values across binary fields
        label_selects = _get_label_selects(labels=labels_map,
                                           repeats=None if ignore_repeat else list(range(12)),
                                           cte=stmt_labels_base)

        # Finally construct the main query
        stmt_labels = (
            sa.select(stmt_labels_base.c.item_id,
                      stmt_labels_base.c.user_id,
                      *label_selects)
            .group_by(stmt_labels_base.c.item_id, stmt_labels_base.c.user_id)
            .subquery('labels')
        )
        stmt_items = nql_query.stmt.subquery('items')

        stmt = (
            sa.select(stmt_items.columns,  # type: ignore[call-overload]
                      stmt_labels.columns,
                      sa.func.coalesce(User.username, 'RESOLVED').label('username'))
            .select_from(stmt_items)
            .join(stmt_labels, stmt_labels.c.item_id == stmt_items.c.item_id, isouter=True)
            .join(User, User.user_id == stmt_labels.c.user_id, isouter=True)
            .order_by(stmt_labels.c.item_id, User.username)
        )
    else:
        raise NotImplementedError('This is a bit more tricky, coming up soon.')

    result = (await session.execute(stmt)).mappings().all()

    return [dict(r) for r in result]


def _generate_keys(key: str, val: dict[str, None | bool | int | list[int]]) -> Generator[tuple[str, bool | str], None, None]:
    if val['bool'] is not None:
        yield f'{key}:{int(val['bool'])}', True  # type: ignore[arg-type]
    elif val['int'] is not None:
        yield f'{key}:{val['int']}', True
    elif val['multi'] is not None:
        for vi in val['multi']:  # type: ignore[union-attr]
            yield f'{key}:{vi}', True
    elif val['str'] is not None:
        yield f'STR|{key}', val['str']
    else:
        raise RuntimeError('No annotation in label')


@ensure_session_async
async def wide_export_table(session: DBSession | AsyncSession,
                            nql_filter: NQLFilter | None,
                            scope_ids: list[str] | list[uuid.UUID],
                            project_id: str | uuid.UUID,
                            limit: int | None = None,
                            prefix: dict[str, str] | None = None,
                            include_meta: bool = False) -> tuple[list[str], list[str], 'pd.DataFrame']:
    import pandas as pd
    if prefix is None:
        prefix = {}

    stmt_labels = sa.text('''
        WITH
            scopes as (
                SELECT scope_id::uuid,
                       row_number() OVER () AS scope_order
                FROM unnest(:scopes ::uuid[]) as scope_id),
            labels_flat as (
                SELECT ba.item_id,
                       ba."order",
                       scope.scope_order,
                       json_object_agg(ba.key, json_build_object('bool', ba.value_bool, 'int', ba.value_int, 'multi', ba.multi_int, 'str', ba.value_str)) as label
                FROM bot_annotation ba
                     JOIN scopes scope ON scope.scope_id = ba.bot_annotation_metadata_id
                GROUP BY ba.item_id, ba."order", scope.scope_order),
            labels as (
                SELECT item_id,
                       min(scope_order) as scope_order,
                       min("order")     as item_order,
                       json_agg(label)  as labels
                FROM labels_flat
                GROUP BY item_id),
            ulabels_flat as (
                SELECT ass.item_id,
                       ass."order",
                       scope.scope_order,
                       u.username,
                       json_object_agg(a.key, json_build_object('bool', a.value_bool, 'int', a.value_int, 'multi', a.multi_int, 'str', a.value_str)) as label
                FROM annotation a
                     JOIN "user" u ON u.user_id = a.user_id
                     JOIN assignment ass ON a.item_id = ass.item_id
                     JOIN scopes scope ON scope.scope_id = ass.assignment_scope_id
                GROUP BY ass.item_id, ass."order", scope.scope_order, u.username),
            ulabels as (
                SELECT item_id,
                       min(scope_order)                 as scope_order,
                       min("order")                     as item_order,
                       json_object_agg(username, label) as labels
                FROM ulabels_flat
                GROUP BY item_id)
        SELECT labels.labels                                     as labels_resolved,
               ulabels.labels                                    as labels_unresolved,
               coalesce(labels.scope_order, ulabels.scope_order) as scope_order,
               coalesce(labels.item_order, ulabels.item_order)   as item_order,
               coalesce(labels.item_id, ulabels.item_id)         as item_id
        FROM labels FULL OUTER JOIN ulabels ON labels.item_id = ulabels.item_id
    ''').columns(
        sa.column('scope_order', sa.Integer),
        sa.column('item_order', sa.Integer),
        sa.column('item_id', psa.UUID),
        sa.column('labels_resolved', psa.JSONB),
        sa.column('labels_unresolved', psa.JSONB),
    ).alias('annotations')

    nql = await NQLQuery.get_query(session=session, query=nql_filter, project_id=str(project_id))

    # stmt_items = nql.stmt
    # rslt = (await session.execute(stmt_items, {'scopes': scope_ids})).mappings().all()
    stmt_items = nql.stmt.subquery()
    stmt = (sa.select(stmt_items, stmt_labels)
            .join(stmt_labels, stmt_labels.c.item_id == stmt_items.c.item_id, isouter=True)
            .order_by(stmt_labels.c.scope_order, stmt_labels.c.item_order))
    if limit:
        stmt = stmt.limit(limit)

    rslt = (await session.execute(stmt, {'scopes': scope_ids})).mappings().all()
    logger.debug(f'Result lines (limit: {limit}) from DB: {len(rslt):,}')

    df = pd.DataFrame([{
        'scope_order': r.get('scope_order'),
        'item_order': r.get('item_order'),
        'item_id': str(r['item_id']),
        'text': f"{r.get('title', '')} {r.get('text', '')} {r.get('teaser', '')}",
        'wos_id': r.get('wos_id'),
        'oa_id': r.get('openalex_id'),
        'doi': r.get('doi'),
        'py': r.get('publication_year'),
        'meta': r,
        **{
            f'res|{prefix.get(k, '')}{key}': val
            for resolution in get_attr(r, 'labels_resolved', [])  # type: ignore[union-attr]
            for k, v in resolution.items()
            for key, val in _generate_keys(k, v)
        },
        **{
            f'{usr}|{prefix.get(k, '')}{key}': val
            for usr, annotation in get_attr(r, 'labels_unresolved', {}).items()  # type: ignore[union-attr]
            for k, v in annotation.items()
            for key, val in _generate_keys(k, v)
        },
    }
        for r in rslt
    ])
    base_cols = ['scope_order', 'item_order', 'item_id', 'text', 'wos_id', 'oa_id', 'doi', 'py']
    if include_meta:
        base_cols += ['meta']
    else:
        df.drop(columns=['meta'], inplace=True)
    str_cols = [col for col in df.columns if '|STR|' in col]
    base_cols += str_cols
    label_cols = list(sorted(set(df.columns) - set(base_cols)))

    df[label_cols] = df[label_cols].astype('Int8')
    df['py'] = df['py'].astype('Int16')
    df['item_order'] = df['item_order'].astype('Int64').astype('Int32')
    df['scope_order'] = df['scope_order'].astype('Int64').astype('Int16')

    # Setting implicit False values to False (instead of leaving them empty)
    anycols: dict[str, list[str]] = defaultdict(list)
    for col in label_cols:
        if '|' in col:
            anycols[col.split(':')[0]].append(col)

    for colgrp, cols in anycols.items():
        logger.debug(f'Resolving implicit False values for {colgrp}: [{cols}]')
        base = anding([df[col].isna() for col in cols])
        if base is not None:
            df[cols] = df[cols].where(~(df[cols].isna() & ~base.to_numpy()[:, None]), other=0)

    # df = df.replace({np.nan: None})

    return base_cols, label_cols, df.reindex(base_cols + label_cols, axis=1)
