import uuid
from typing import Any

from sqlalchemy import text

from nacsos_data.db.engine import ensure_session_async, DBSession
from nacsos_data.models.annotations import AnnotationSchemeModel, AnnotationSchemeLabel, Label, ItemAnnotation
from nacsos_data.models.bot_annotations import ResolutionSnapshotEntry, ResolutionMatrix, SnapshotEntry, OrderingEntry, BotItemAnnotation


def unravel_annotation_scheme_keys(scheme: AnnotationSchemeModel) -> list[str]:
    def recurse_label(label: AnnotationSchemeLabel | None, accu: list[str]) -> list[Any]:
        if label is None:
            return accu
        accu += [label.key]
        if label.choices is None:
            return accu

        return [recurse_label(child, accu) for choice in label.choices if choice is not None and choice.children is not None for child in choice.children]

    keys = [recurse_label(label, []) for label in scheme.labels]
    return [k for kk in keys for k in kk]


@ensure_session_async
async def get_ordering(session: DBSession, assignment_scope_id: str | uuid.UUID) -> list[OrderingEntry]:
    """
    Retrieve all items from this assignment scope, order them by the order of assignments and attach some
    basic information about the state of the assignment.

    :param session:
    :param assignment_scope_id: assignment_scope_id
    :return:
    """
    stmt = text("""
        SELECT row_number() over () as identifier, *
        FROM (SELECT ass.item_id::text,
                     MIN(ass."order") as first_occurrence,
                     array_agg(jsonb_build_object(
                             'assignment_id', ass.assignment_id::text,
                             'item_id', ass.item_id::text,
                             'user_id', ass.user_id::text,
                             'username', u.username,
                             'status', ass.status,
                             'order', ass."order"
                         ))           as assignments
              FROM assignment ass
                   JOIN "user" u on u.user_id = ass.user_id
              WHERE ass.assignment_scope_id = :scope_id
              GROUP BY ass.item_id
              ORDER BY first_occurrence) as sub;
    """)
    res = (await session.execute(stmt, {'scope_id': assignment_scope_id})).mappings().all()

    return [OrderingEntry(**r) for r in res]


def dehydrate_user_annotations(matrix: ResolutionMatrix) -> list[SnapshotEntry]:
    return [
        SnapshotEntry(
            order_key=row_key,
            path_key=col_key,
            user_id=str(user_id),
            item_id=str(anno.annotation.item_id),
            anno_id=str(anno.annotation.annotation_id),
            value_str=anno.annotation.value_str,
            value_bool=anno.annotation.value_bool,
            value_int=anno.annotation.value_int,
            value_float=anno.annotation.value_float,
            multi_int=anno.annotation.multi_int,
        )
        for row_key, row in matrix.items()
        for col_key, cell in row.items()
        for user_id, annos in cell.labels.items()
        for anno in annos
        if anno.annotation
    ]


def dehydrate_resolutions(matrix: ResolutionMatrix) -> list[ResolutionSnapshotEntry]:
    return [
        ResolutionSnapshotEntry(order_key=row_key, path_key=col_key, ba_id=str(cell.resolution.bot_annotation_id))
        for row_key, row in matrix.items()
        for col_key, cell in row.items()
    ]


@ensure_session_async
async def read_item_annotations(
    session: DBSession, assignment_scope_id: str | uuid.UUID, ignore_hierarchy: bool = False, ignore_repeat: bool = False
) -> list[ItemAnnotation]:
    """
    asd
    :param session: Connection to the database
    :param assignment_scope_id:
    :param ignore_hierarchy: if False, looking at keys linearly (ignoring parents)
    :param ignore_repeat: if False, the order is ignored and e.g. single-choice with secondary category
                           virtually becomes multi-choice of two categories
    :return: dictionary (keys are item_ids) of all annotations per item that match the filters.
    """

    repeat = 'a.repeat'
    if ignore_repeat:  # if repeat is ignored, always forcing it to 1
        repeat = '1'
    if ignore_hierarchy:
        stmt = text(f"""
            SELECT array_to_json(ARRAY[(a.key, {repeat})::annotation_label]) as label,
                   a.*
            FROM annotation AS a
                     JOIN assignment ass ON a.assignment_id = ass.assignment_id
            WHERE ass.assignment_scope_id = :scope_id;
        """)
    else:
        stmt = text(f"""
            WITH RECURSIVE ctename AS (
                  SELECT a.annotation_id, a.time_created, a.time_updated, a.assignment_id, a.user_id, a.item_id,
                         a.annotation_scheme_id, a.key, a.repeat,  a.value_bool, a.value_int,a.value_float,
                         a.value_str, a.multi_int, a.parent, a.parent as recurse_join,
                        ARRAY[(a.key, {repeat})::annotation_label] as path
                  FROM annotation AS a
                           JOIN assignment ass ON a.assignment_id = ass.assignment_id
                  WHERE ass.assignment_scope_id = :scope_id
               UNION ALL
                  SELECT ctename.annotation_id, ctename.time_created, ctename.time_updated,ctename.assignment_id,
                         ctename.user_id, ctename.item_id, ctename.annotation_scheme_id, ctename.key,
                         ctename.repeat, ctename.value_bool, ctename.value_int,ctename.value_float,
                         ctename.value_str, ctename.multi_int, ctename.parent, a.parent as recurse_join,
                        array_append(ctename.path, ((a.key, {repeat})::annotation_label))
                  FROM annotation a
                     JOIN ctename ON a.annotation_id = ctename.recurse_join
            )
            SELECT array_to_json(path) as label, ctename.*
            FROM ctename
            WHERE recurse_join is NULL;
        """)

    res = (await session.execute(stmt, {'scope_id': assignment_scope_id})).mappings().all()
    ret = []
    for r in res:
        path = [Label.model_validate(label) for label in r['label']]
        ret.append(ItemAnnotation(**{**r, 'path': path}))
    return ret


@ensure_session_async
async def read_bot_annotations(session: DBSession, bot_annotation_metadata_id: str) -> list[BotItemAnnotation]:
    stmt = text("""
        WITH RECURSIVE ctename AS (
              SELECT a.bot_annotation_id, a.bot_annotation_metadata_id,
                     a.time_created, a.time_updated, a.item_id,
                     a.key, a.repeat, a.confidence, a.order,
                     a.value_bool, a.value_int, a.value_float, a.multi_int, a.value_str,
                     a.parent, a.parent as recurse_join,
                    ARRAY[(a.key, a.repeat)::annotation_label] as path
              FROM bot_annotation AS a
              WHERE a.bot_annotation_metadata_id = :bot_annotation_metadata_id
           UNION ALL
              SELECT ctename.bot_annotation_id,  a.bot_annotation_metadata_id,
                     ctename.time_created, ctename.time_updated, ctename.item_id,
                     ctename.key, ctename.repeat, ctename.confidence, ctename.order,
                     ctename.value_bool, ctename.value_int, ctename.value_float, ctename.multi_int, ctename.value_str,
                     ctename.parent, a.parent as recurse_join,
                    array_append(ctename.path, ((a.key, a.repeat)::annotation_label))
              FROM bot_annotation AS a
                 JOIN ctename ON a.bot_annotation_id = ctename.recurse_join
        )
        SELECT array_to_json(path) as label, ctename.*
        FROM ctename
        WHERE recurse_join is NULL
        ORDER BY ctename.order;
    """)

    res = (await session.execute(stmt, {'bot_annotation_metadata_id': bot_annotation_metadata_id})).mappings().all()
    ret = []
    for r in res:
        path = [Label.model_validate(label) for label in r['label']]
        ret.append(BotItemAnnotation(**{**r, 'path': path}))
    return ret
