import uuid
import logging

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.connection import DatabaseEngineAsync
from nacsos_data.db.engine import ensure_session_async, DBSession
from nacsos_data.db.schemas import User, Project
from nacsos_data.db.schemas.annotations import AssignmentScope, AnnotationScheme
from nacsos_data.models.nql import NQLFilter
from nacsos_data.util.errors import NotFoundError
from nacsos_data.util.nql import NQLQuery
from nacsos_data.util import pluck
from nacsos_data.db.crud.users import read_users
from .util import LabelOptions, _labels_subquery, _get_label_selects
from nacsos_data.db.crud.annotations import read_assignment_scopes_for_project_info, read_resolution_scopes_for_project_info

logger = logging.getLogger('nacsos_data.util.annotations.export')


async def get_labels(stmt_labels: sa.CTE, db_engine: DatabaseEngineAsync) -> dict[str, LabelOptions]:
    stmt_labels_ = sa.union(
        sa.select(
            stmt_labels.c.key,
            stmt_labels.c.value_int,
            stmt_labels.c.value_bool,
            stmt_labels.c.value_str,
            sa.func.unnest(stmt_labels.c.multi_int).label('multis'),
        ),
        sa.select(
            stmt_labels.c.key,
            stmt_labels.c.value_int,
            stmt_labels.c.value_bool,
            stmt_labels.c.value_str,
            sa.literal(None, type_=sa.Integer).label('multis'),
        ),
    ).subquery()

    stmt_options = (
        sa.select(
            stmt_labels_.c.key,
            sa.func.array_agg(sa.distinct(stmt_labels_.c.value_int)).filter(stmt_labels_.c.value_int.isnot(None)).label('options_int'),
            sa.func.array_agg(sa.distinct(stmt_labels_.c.value_bool)).filter(stmt_labels_.c.value_bool.isnot(None)).label('options_bool'),
            sa.func.array_agg(sa.distinct(stmt_labels_.c.multis)).filter(stmt_labels_.c.multis.isnot(None)).label('options_multi'),
            (sa.func.count().filter(stmt_labels_.c.value_str.isnot(None)) > 0).label('strings'),
        )
        .where(stmt_labels_.c.key.isnot(None))
        .group_by(stmt_labels_.c.key)
        .order_by(stmt_labels_.c.key)
    )

    session: AsyncSession
    async with db_engine.session() as session:
        result = (await session.execute(stmt_options)).mappings().all()

        # construct a lookup map of key->options/values/choices
        return {row['key']: LabelOptions.model_validate(row) for row in result}


async def get_project_labels(project_id: str | uuid.UUID, db_engine: DatabaseEngineAsync) -> dict[str, LabelOptions]:
    project_bot_scopes = await read_resolution_scopes_for_project_info(project_id=project_id, db_engine=db_engine)
    project_scopes = await read_assignment_scopes_for_project_info(project_id=project_id, db_engine=db_engine)
    project_users = (await read_users(project_id=str(project_id), engine=db_engine)) or []

    bot_annotation_metadata_ids: list[str] = list(pluck(project_bot_scopes, 'scope_id'))
    assignment_scope_ids: list[str] = list(pluck(project_scopes, 'scope_id'))
    user_ids: list[str] = list(pluck(project_users, 'user_id'))

    stmt_labels = _labels_subquery(
        bot_annotation_metadata_ids=bot_annotation_metadata_ids,
        assignment_scope_ids=assignment_scope_ids,
        user_ids=user_ids,
        labels=None,
        ignore_repeat=True,
    )

    return await get_labels(stmt_labels=stmt_labels, db_engine=db_engine)


@ensure_session_async
async def prepare_export_table(
    session: DBSession | AsyncSession,
    nql_filter: NQLFilter | None,
    bot_annotation_metadata_ids: list[str] | list[uuid.UUID] | None,
    assignment_scope_ids: list[str] | list[uuid.UUID] | None,
    user_ids: list[str] | list[uuid.UUID] | None,
    project_id: str | uuid.UUID,
    labels: list[LabelOptions],
    ignore_hierarchy: bool,
    ignore_repeat: bool,
    max_results: int | None = None,
) -> list[dict[str, bool | int | str | None]]:
    project_type = await session.scalar(sa.select(Project.type).where(Project.project_id == project_id))

    if project_type is None:
        raise NotFoundError(f'No project with id={project_id}!')

    nql_query = NQLQuery(query=nql_filter, project_id=str(project_id), project_type=project_type)

    if max_results is not None and max_results > 0:
        stmt = nql_query.stmt.subquery()
        cnt_stmt = sa.func.count(stmt.c.item_id)
        count = (await session.execute(cnt_stmt)).scalar() or 0
        logger.info(f'Found {count:,} items for query')
        if count > max_results or count < 1:
            raise RuntimeError(f'Found {count:,} items for query (0 > count > {max_results:,})')

    labels_map = {lab.key: lab for lab in labels}
    stmt_labels_base = _labels_subquery(
        bot_annotation_metadata_ids=bot_annotation_metadata_ids,
        assignment_scope_ids=assignment_scope_ids,
        user_ids=user_ids,
        labels=labels_map,
        ignore_repeat=ignore_repeat,
    )

    if ignore_hierarchy:
        # Prepare the CASE expressions to spread label values across binary fields
        label_selects = _get_label_selects(labels=labels_map, repeats=None if ignore_repeat else list(range(12)), cte=stmt_labels_base)

        # Finally construct the main query
        stmt_labels = (
            sa.select(stmt_labels_base.c.item_id, stmt_labels_base.c.user_id, *label_selects)
            .group_by(stmt_labels_base.c.item_id, stmt_labels_base.c.user_id)
            .subquery('labels')
        )
        stmt_items = nql_query.stmt.subquery('items')

        stmt_items_columns = [c for c in stmt_items.columns if c.key not in ('item_id_1', 'project_id_1')]
        stmt_labels_columns = [c for c in stmt_labels.columns if c.key != 'item_id']

        result_stmt = (
            sa.select(
                *stmt_items_columns,
                *stmt_labels_columns,
                sa.case(
                    (stmt_labels.c.item_id.isnot(None), sa.func.coalesce(User.username, 'RESOLVED')),
                    else_=sa.null(),
                ).label('username'),
            )
            .select_from(stmt_items)
            .join(stmt_labels, stmt_labels.c.item_id == stmt_items.c.item_id, isouter=True)
            .join(User, User.user_id == stmt_labels.c.user_id, isouter=True)
            .order_by(stmt_labels.c.item_id, User.username)
        )
    else:
        raise NotImplementedError('This is a bit more tricky, coming up soon.')

    result = (await session.execute(result_stmt)).mappings().all()

    return [dict(r) for r in result]


@ensure_session_async
async def get_labels_with_names(session: DBSession | AsyncSession, scopes: list[str] | list[uuid.UUID]) -> dict[str, tuple[str, str]]:
    # get annotation_labels by scope_id
    stmt = (
        sa.select(AnnotationScheme.annotation_scheme_id, AnnotationScheme.labels)
        .join(
            AssignmentScope,
            AnnotationScheme.annotation_scheme_id == AssignmentScope.annotation_scheme_id,
        )
        .where(AssignmentScope.assignment_scope_id.in_(scopes))
        .distinct()
    )

    schemes = (await session.execute(stmt)).all()

    if len(schemes) != 1:
        raise AssertionError('Found more than one or no scheme for the provided scopes.')

    # prepare labels with names
    label_mappings: dict[str, tuple[str, str]] = {}

    def _add_label_mapping(key: str, value: tuple[str, str]) -> None:
        if key in label_mappings:
            raise ValueError(f'Invalid annotation scheme! Duplicate label mapping {key!r}: existing={label_mappings[key]!r}, new={value!r}')
        label_mappings[key] = value

    for label in schemes[0].labels:
        if label['kind'] == 'str':
            continue
        if label['kind'] in {'single', 'multi'}:
            for choice in label['choices']:
                _add_label_mapping(f'{label["key"]}|{choice["value"]}', (label['name'], choice['name']))
        elif label['kind'] == 'bool':
            _add_label_mapping(f'{label["key"]}|1', (label['name'], label['name']))
            _add_label_mapping(f'{label["key"]}|0', (label['name'], f'Not {label["name"]}'))
        else:
            raise KeyError(f'Unknown label type {label["kind"]}: {label}')

    return label_mappings
