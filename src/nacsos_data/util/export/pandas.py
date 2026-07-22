import uuid
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Generator

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psa
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.engine import ensure_session_async, DBSession
from nacsos_data.models.nql import NQLFilter

from .. import get, anding
from ..nql import NQLQuery


if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger('nacsos_data.util.annotations.export')


def _generate_keys(key: str, val: dict[str, None | bool | int | list[int]]) -> Generator[tuple[str, bool | str], None, None]:
    if val['bool'] is not None:
        yield f'{key}:{int(val["bool"])}', True  # type: ignore[arg-type]
    elif val['int'] is not None:
        yield f'{key}:{val["int"]}', True
    elif val['multi'] is not None:
        for vi in val['multi']:  # type: ignore[union-attr]
            yield f'{key}:{vi}', True
    elif val['str'] is not None:
        yield f'STR|{key}', val['str']  # type: ignore[misc]
    else:
        raise RuntimeError('No annotation in label')


@ensure_session_async
async def wide_export_table(
    session: DBSession | AsyncSession,
    nql_filter: NQLFilter | None,
    scope_ids: list[str] | list[uuid.UUID],
    project_id: str | uuid.UUID,
    limit: int | None = None,
    prefix: dict[str, str] | None = None,
    include_meta: bool = False,
) -> tuple[list[str], list[str], 'pd.DataFrame']:
    import pandas as pd

    if prefix is None:
        prefix = {}

    stmt_labels = (
        sa.text("""
                WITH
                    scopes as (
                        SELECT scope_id::uuid,
                               row_number() OVER () AS scope_order
                        FROM unnest(:scopes ::uuid[]) as scope_id),
                    labels_flat as (
                        SELECT ba.item_id,
                               ba."order",
                               scope.scope_order,
                               json_object_agg(ba.key, json_build_object('bool', ba.value_bool, 'int', ba.value_int, 'multi',
                                                                         ba.multi_int, 'str', ba.value_str)) as label
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
                               json_object_agg(a.key,
                                               json_build_object('bool', a.value_bool, 'int', a.value_int, 'multi', a.multi_int,
                                                                 'str', a.value_str)) as label
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
                FROM labels
                     FULL OUTER JOIN ulabels ON labels.item_id = ulabels.item_id
                """)
        .columns(
            sa.column('scope_order', sa.Integer),
            sa.column('item_order', sa.Integer),
            sa.column('item_id', psa.UUID),
            sa.column('labels_resolved', psa.JSONB),
            sa.column('labels_unresolved', psa.JSONB),
        )
        .alias('annotations')
    )

    nql = await NQLQuery.get_query(session=session, query=nql_filter, project_id=str(project_id))

    # stmt_items = nql.stmt
    # rslt = (await session.execute(stmt_items, {'scopes': scope_ids})).mappings().all()
    stmt_items = nql.stmt.subquery()
    stmt = (
        sa.select(stmt_items, stmt_labels)
        .join(stmt_labels, stmt_labels.c.item_id == stmt_items.c.item_id, isouter=True)
        .order_by(stmt_labels.c.scope_order, stmt_labels.c.item_order)
    )
    if limit:
        stmt = stmt.limit(limit)

    rslt = (await session.execute(stmt, {'scopes': scope_ids})).mappings().all()
    logger.debug(f'Result lines (limit: {limit}) from DB: {len(rslt):,}')

    df = pd.DataFrame(
        [
            {
                'scope_order': r.get('scope_order'),
                'item_order': r.get('item_order'),
                'item_id': str(r['item_id']),
                'title': r.get('title'),
                'text': r.get('text'),
                'authors': r.get('authors'),
                'teaser': r.get('teaser'),
                'wos_id': r.get('wos_id'),
                'openalex_id': r.get('openalex_id'),
                'scopus_id': r.get('scopus_id'),
                'source': r.get('source'),
                'publication_date': r.get('publication_date'),
                'doi': r.get('doi'),
                'py': r.get('publication_year'),
                **{
                    f'res|{prefix.get(k, "")}{key}': val
                    for resolution in get(r, 'labels_resolved', default=[])
                    for k, v in resolution.items()
                    for key, val in _generate_keys(k, v)
                },
                **{
                    f'{usr}|{prefix.get(k, "")}{key}': val
                    for usr, annotation in get(r, 'labels_unresolved', default={}).items()
                    for k, v in annotation.items()
                    for key, val in _generate_keys(k, v)
                },
            }
            for r in rslt
        ],
    )
    base_cols = ['scope_order', 'item_order', 'item_id', 'wos_id', 'openalex_id', 'scopus_id', 'doi', 'title', 'text', 'teaser', 'authors', 'source', 'py']
    if include_meta:
        base_cols += ['meta']
    else:
        df.drop(columns=['meta'], inplace=True, errors='ignore')
    str_cols = [col for col in df.columns if '|STR|' in col]
    base_cols += str_cols
    label_cols = sorted(set(df.columns) - set(base_cols))

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
            df[cols] = df[cols].where(~(df[cols].isna() & ~base.to_numpy()[:, None]), other=0)  # type: ignore[union-attr]

    # df = df.replace({np.nan: None})

    return base_cols, label_cols, df.reindex(base_cols + label_cols, axis=1)
