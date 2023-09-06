import json
import logging
import uuid
from datetime import datetime
from collections import defaultdict

from pydantic import BaseModel
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.connection import DatabaseEngineAsync
from nacsos_data.db.crud.annotations import read_annotation_scheme
from nacsos_data.db.engine import ensure_session_async
from nacsos_data.db.schemas import AssignmentScope
from nacsos_data.models.annotations import (
    AnnotationModel,
    AnnotationSchemeModel,
    AnnotationSchemeInfo
)
from nacsos_data.models.bot_annotations import (
    Label,
    AnnotationFilters,
    AnnotationFiltersType,
    ResolutionMethod,
    GroupedBotAnnotation,
    BotAnnotationModel,
    ResolutionUserEntry,
    ResolutionCell,
    ResolutionMatrix,
    OrderingEntry, ResolutionOrdering
)
from nacsos_data.models.users import UserModel
from nacsos_data.util.annotations.resolve.majority_vote import naive_majority_vote
from nacsos_data.util.annotations.validation import (
    labels_from_scheme,
    path_to_string,
    FlatLabel
)
from nacsos_data.util.errors import NotFoundError, InvalidFilterError

logger = logging.getLogger('nacsos_data.util.annotations.resolve')


class ItemAnnotation(AnnotationModel):
    path: list[Label]


class AnnotationFilterObject(AnnotationFilters):
    def get_subquery(self) -> tuple[str, str, AnnotationFiltersType]:
        where = []
        filters = self.get_filters()
        for db_col, key in [('ass.assignment_scope_id', 'scope_id'),
                            ('a.annotation_scheme_id', 'scheme_id'),
                            ('a.user_id', 'user_id'),
                            ('a.key', 'key'),
                            ('a.repeat', 'repeat')]:
            if filters.get(key) is not None:
                if type(filters[key]) == list:
                    where.append(f' {db_col} = ANY(:{key}) ')
                else:
                    where.append(f' {db_col} = :{key} ')

        if len(where) == 0:
            raise InvalidFilterError('You did not specify any valid filter.')

        join = f''
        if filters.get('scope_id') is not None:
            join = f' JOIN assignment ass on ass.assignment_id = a.assignment_id '

        return join, ' AND '.join(where), filters

    def get_filters(self) -> AnnotationFiltersType:
        ret = {}
        for key, value in self.model_dump().items():
            if value is not None:
                if type(value) == list:
                    if len(value) == 1:
                        ret[key] = value[0]
                    else:
                        ret[key] = list(value)
                else:
                    ret[key] = value
        return ret


@ensure_session_async
async def read_num_annotation_changes_after(timestamp: str | datetime,
                                            filters: AnnotationFilterObject,
                                            session: AsyncSession) -> int:
    """
    Looks for `Annotation`s that were added or edited after `timestamp` and returns the count.
    This is assumed to be used in the context of `Annotation` resolution, so the same filter logic is used, so
    it can be re-applied later to see if there have been changes that should be included in the resolution.
    NOTICE: This does *not* recognise deletions (but `Annotation`s should never be deleted anyway).
    If you are looking for the actually changed `Annotation`s, use `read_changed_annotations_after`.
    :param timestamp:
    :param filters:
    :param session:
    :return:
    """
    filter_join, filter_where, filter_data = filters.get_subquery()
    filter_data['timestamp'] = str(timestamp)
    num_changes: int = (await session.execute(text(  # type: ignore[assignment]
        "SELECT count(1) "
        "FROM annotation AS a "
        f" {filter_join} "
        f"WHERE {filter_where} "
        f"      AND (a.time_created > :timestamp OR a.time_updated > :timestamp);"
    ), filter_data)).scalar()
    return num_changes


@ensure_session_async
async def read_changed_annotations_after(timestamp: str | datetime,
                                         filters: AnnotationFilterObject,
                                         session: AsyncSession) -> list[AnnotationModel]:
    """
    See `read_num_annotation_changes_after`
    :param timestamp:
    :param filters:
    :param session:
    :return:
    """
    filter_join, filter_where, filter_data = filters.get_subquery()
    filter_data['timestamp'] = str(timestamp)
    annotations = (await session.execute(text(
        "SELECT a.* "
        "FROM annotation AS a "
        f" {filter_join} "
        f"WHERE {filter_where} "
        f"      AND (a.time_created > :timestamp OR a.time_updated > :timestamp);"
    ), filter_data)).mappings().all()
    return [AnnotationModel.model_validate(anno) for anno in annotations]


@ensure_session_async
async def read_item_annotations(filters: AnnotationFilterObject,
                                session: AsyncSession,
                                ignore_hierarchy: bool = False,
                                ignore_order: bool = False) -> list[ItemAnnotation]:
    """
    asd
    :param session: Connection to the database
    :param filters:
    :param ignore_hierarchy: if False, looking at keys linearly (ignoring parents)
    :param ignore_order: if False, the order is ignored and e.g. single-choice with secondary category
                           virtually becomes multi-choice of two categories
    :return: dictionary (keys are item_ids) of all annotations per item that match the filters.
    """
    filter_join, filter_where, filter_data = filters.get_subquery()

    repeat = 'a.repeat'
    if ignore_order:  # if repeat is ignored, always forcing it to 1
        repeat = '1'
    if ignore_hierarchy:
        stmt = text(f'''
            SELECT array_to_json(ARRAY[(a.key, {repeat})::annotation_label]) as path, 
                   a.*
            FROM annotation AS a 
                     {filter_join} 
            WHERE {filter_where};
        ''')
    else:
        stmt = text(f'''
            WITH RECURSIVE ctename AS ( 
                  SELECT a.annotation_id, a.time_created, a.time_updated, a.assignment_id, a.user_id, a.item_id, 
                         a.annotation_scheme_id, a.key, a.repeat,  a.value_bool, a.value_int,a.value_float, 
                         a.value_str, a.text_offset_start, a.text_offset_stop, a.multi_int, 
                         a.parent, a.parent as recurse_join, 
                        ARRAY[(a.key, {repeat})::annotation_label] as path 
                  FROM annotation AS a 
                     {filter_join} 
                  WHERE {filter_where} 
               UNION ALL 
                  SELECT ctename.annotation_id, ctename.time_created, ctename.time_updated,ctename.assignment_id,
                         ctename.user_id, ctename.item_id, ctename.annotation_scheme_id, ctename.key, 
                         ctename.repeat, ctename.value_bool, ctename.value_int,ctename.value_float, 
                         ctename.value_str, ctename.text_offset_start, ctename.text_offset_stop, 
                         ctename.multi_int, ctename.parent, a.parent as recurse_join, 
                        array_append(ctename.path, ((a.key, {repeat})::annotation_label)) 
                  FROM annotation a 
                     JOIN ctename ON a.annotation_id = ctename.recurse_join 
            ) 
            SELECT array_to_json(path) as label, ctename.*
            FROM ctename 
            WHERE recurse_join is NULL;
        ''')

    res = (await session.execute(stmt, filter_data)).mappings().all()
    ret = []
    for r in res:
        path = [Label.model_validate(label) for label in r['label']]
        ret.append(ItemAnnotation(**{**r, 'path': path}))
    return ret


@ensure_session_async
async def read_scopes_for_scheme(scheme_id: str | uuid.UUID, session: AsyncSession) -> list[str]:
    stmt = select(AssignmentScope.assignment_scope_id).where(AssignmentScope.annotation_scheme_id == scheme_id)
    return [str(scope_id) for scope_id in (await session.execute(stmt)).scalars().all()]


@ensure_session_async
async def read_annotators(filters: AnnotationFilterObject, session: AsyncSession) -> list[UserModel]:
    # list of all (unique) users that have at least one annotation in the set
    filter_join, filter_where, filter_data = filters.get_subquery()
    return [UserModel.model_validate(user) for user in (await session.execute(text(
        "SELECT DISTINCT u.* "
        "FROM annotation AS a "
        f"   {filter_join} "
        "    JOIN \"user\" u on u.user_id = a.user_id "
        f"WHERE {filter_where};"
    ), filter_data)).mappings().all()]


@ensure_session_async
async def read_labels(filters: AnnotationFilterObject,
                      session: AsyncSession,
                      ignore_hierarchy: bool = True,
                      ignore_order: bool = True) -> list[list[Label]]:
    # list of all (unique) labels in this selection
    repeat = 'a.repeat'
    if ignore_order:  # if repeat is ignored, always forcing it to 1
        repeat = '1'
    filter_join, filter_where, filter_data = filters.get_subquery()
    if ignore_hierarchy:
        return [[Label.model_validate(sub_label) for sub_label in label]
                for label in
                (await session.execute(text(
                    "SELECT array_to_json(label) as label "
                    "FROM ( "
                    f"   SELECT DISTINCT ARRAY[(a.key, {repeat})::annotation_label] as label "
                    "    FROM annotation AS a "
                    f"      {filter_join} "
                    f"   WHERE {filter_where}) labels;"
                ), filter_data)).scalars()]
    else:
        return [[Label.model_validate(sub_label) for sub_label in label]
                for label in
                (await session.execute(text(
                    "SELECT array_to_json(label) as label "
                    "FROM ( "
                    "WITH RECURSIVE ctename AS ( "
                    f"      SELECT a.annotation_id, a.parent, ARRAY[(a.key, {repeat})::annotation_label] as label "
                    "       FROM annotation AS a "
                    f"         {filter_join} "
                    f"      WHERE {filter_where} "
                    "   UNION ALL "
                    "      SELECT a.annotation_id, a.parent,"
                    f"             array_append(ctename.label, ((a.key, {repeat})::annotation_label)) "
                    "      FROM annotation a "
                    "         JOIN ctename ON a.annotation_id = ctename.parent "
                    ") "
                    "SELECT DISTINCT label "
                    "FROM ctename "
                    "WHERE parent is NULL) labels;"
                ), filter_data)).scalars()]


@ensure_session_async
async def read_bot_annotations(bot_annotation_metadata_id: str,
                               session: AsyncSession) -> dict[str, list[GroupedBotAnnotation]]:
    bot_annotations = (await session.execute(text(
        "WITH RECURSIVE ctename AS ( "
        "    SELECT a.bot_annotation_id, a.bot_annotation_metadata_id, a.time_created, a.time_updated, a.item_id, "
        "           a.key, a.repeat,  a.value_bool, a.value_int,a.value_float, "
        "           a.value_str, a.multi_int, a.confidence, a.parent, a.parent as recurse_join, "
        "           ARRAY [(a.key, a.repeat)::annotation_label] as path "
        "       FROM bot_annotation AS a "
        "       WHERE a.bot_annotation_metadata_id = :bot_annotation_metadata_id "
        "    UNION ALL "
        "      SELECT ctename.bot_annotation_id, ctename.bot_annotation_metadata_id, ctename.time_created, "
        "             ctename.time_updated, ctename.item_id, ctename.key, ctename.repeat,  ctename.value_bool, "
        "             ctename.value_int,ctename.value_float, ctename.value_str, ctename.multi_int, "
        "             ctename.confidence, ctename.parent, a.parent as recurse_join, "
        "             array_append(ctename.path, ((a.key, a.repeat)::annotation_label)) "
        "       FROM bot_annotation a "
        "            JOIN ctename ON a.bot_annotation_id = ctename.recurse_join) "
        "SELECT item_id, array_to_json(path) as label, json_agg(ctename.*)::jsonb->0 as bot_annotation "
        "FROM ctename "
        "WHERE recurse_join is NULL "
        "GROUP BY item_id, path;"),
        {'bot_annotation_metadata_id': bot_annotation_metadata_id})).mappings().all()

    grouped_annotations = defaultdict(list)
    for ba in bot_annotations:
        grouped_annotations[str(ba['item_id'])].append(
            GroupedBotAnnotation(path=ba['label'],
                                 annotation=BotAnnotationModel.model_validate(ba['bot_annotation'])))
    return grouped_annotations


@ensure_session_async
async def get_ordering(scope_id: str | uuid.UUID, session: AsyncSession) -> list[OrderingEntry]:
    stmt = text('''
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
        ORDER BY first_occurrence) as sub;''')
    res = (await session.execute(stmt, {'scope_id': scope_id})).mappings().all()

    return [OrderingEntry(**r, key=f'{scope_id}|{r.item_id}', scope_id=str(scope_id)) for r in res]


class ResolutionProposal(BaseModel):
    scheme_info: AnnotationSchemeInfo
    labels: list[FlatLabel]
    annotators: list[UserModel]
    ordering: list[ResolutionOrdering]
    matrix: ResolutionMatrix


async def get_resolved_item_annotations(strategy: ResolutionMethod,
                                        filters: AnnotationFilterObject,
                                        db_engine: DatabaseEngineAsync,
                                        ignore_hierarchy: bool = False,
                                        ignore_order: bool = False,
                                        include_empty: bool = True,
                                        existing_resolution: str = None,
                                        include_new: bool = False,
                                        update_existing: bool = False) -> ResolutionProposal:
    """
    This method retrieves all annotations that match the selected filters and constructs a matrix
    of annotations per item (rows) and label (columns).
    The "cells" contain annotations by each user and the resolution.
    The method also returns additional

    :param strategy: Resolution strategy (e.g. majority vote of available user annotations)
    :param filters: see `AnnotationFilterObject`
    :param db_engine:
    :param ignore_hierarchy: When True, this will respect the nested nature of the annotation scheme
    :param ignore_order: When True, this will ignore the order (`repeat`, e.g. primary, secondary,...) of annotations
    :param include_empty: Should items without annotations be included?
    :param existing_resolution: When set, will load an existing resolution and include its data here
    :param include_new: When `existing_resolution` is set, should I include new items?
    :param update_existing: When `existing_resolution` is set, should I update existing resolutions?
    :return:
    """
    logger.debug(f'Fetching all annotations matching filters: {filters} '
                 f'with ignore_hierarchy={ignore_hierarchy} and ignore_order={ignore_order}.')
    filter_keys = [filters.key] if type(filters.key) is str else filters.key
    filter_repeats = [filters.repeat] if type(filters.repeat) is str else filters.repeat
    filter_scopes = [filters.scope_id] if type(filters.scope_id) is str else filters.scope_id
    scheme_id = filters.scheme_id
    async with db_engine.session() as session:  # type: AsyncSession
        scheme: AnnotationSchemeModel = await read_annotation_scheme(annotation_scheme_id=scheme_id,
                                                                     session=session)
        if not scheme:
            raise NotFoundError(f'No annotation scheme for {scheme_id}')

        annotators = await read_annotators(filters=filters,
                                           session=session)
        labels = labels_from_scheme(scheme,
                                    ignore_hierarchy=ignore_hierarchy,
                                    ignore_order=ignore_order,
                                    keys=filter_keys,
                                    repeats=filter_repeats)
        label_map = {l.path_key: l for l in labels}
        if filter_scopes is None:
            filter_scopes = await read_scopes_for_scheme(scheme_id=filters.scheme_id, session=session)

        item_order: list[OrderingEntry] = []
        for scope_id in filter_scopes:
            item_order += await get_ordering(scope_id=scope_id, session=session)

        annotations: list[ItemAnnotation] = await read_item_annotations(session=session,
                                                                        filters=filters,
                                                                        ignore_hierarchy=ignore_hierarchy,
                                                                        ignore_order=ignore_order)
        logger.debug(f'Got {len(labels)} labels, {len(annotators)} annotators, {len(item_order):,} items '
                     f'and {len(annotations):,} annotations.')
        assignments = {
            str(ass.assignment_id): (ass, order_entry)
            for order_entry in item_order
            for ass in order_entry.assignments
        }
        annotation_map: ResolutionMatrix = {}
        for annotation in annotations:
            assignment, order_entry = assignments[str(annotation.assignment_id)]
            row_key = order_entry.key
            col_key = path_to_string(annotation.path)
            user_id = str(annotation.user_id)
            if row_key not in annotation_map:
                annotation_map[row_key] = {}
            if col_key not in annotation_map[row_key]:
                annotation_map[row_key][col_key] = ResolutionCell(labels={})
            if user_id not in annotation_map[row_key][col_key].labels:
                annotation_map[row_key][col_key].labels[user_id] = []
            entry = ResolutionUserEntry(annotation=annotation)
            if str(annotation.assignment_id) is not None and str(annotation.assignment_id) in assignments:
                entry.assignment = assignment
            annotation_map[row_key][col_key].labels[user_id].append(entry)

        # TODO compare existing and update status
        # TODO drop empty
        print(annotation_map.keys())
        if strategy == 'majority':
            annotation_map = naive_majority_vote(annotation_map, label_map)
        else:
            raise NotImplementedError(f'Resolution strategy "{strategy}" not implemented (yet)!')

        return ResolutionProposal(
            scheme_info=AnnotationSchemeInfo(**scheme.model_dump()),
            labels=labels,
            annotators=annotators,
            ordering=[ResolutionOrdering(**o.model_dump()) for o in item_order],
            matrix=annotation_map
        )
