import logging
import uuid
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.crud.annotations import read_annotation_scheme
from nacsos_data.db.engine import ensure_session_async
from nacsos_data.db.schemas import AssignmentScope, User
from nacsos_data.models.annotations import (
    AnnotationSchemeModel,
    AnnotationSchemeInfo,
    ItemAnnotation,
    FlatLabel,
    AnnotationValue
)
from nacsos_data.models.bot_annotations import (
    Label,
    AnnotationFilters,
    AnnotationFiltersType,
    ResolutionMethod,
    BotAnnotationModel,
    ResolutionUserEntry,
    ResolutionCell,
    ResolutionMatrix,
    OrderingEntry,
    ResolutionOrdering,
    BotItemAnnotation,
    ResolutionProposal,
    BotAnnotationResolution,
    ResolutionStatus,
    AssignmentMap
)
from nacsos_data.models.users import UserModel
from nacsos_data.util.annotations.resolve.majority_vote import naive_majority_vote
from nacsos_data.util.annotations.validation import (
    labels_from_scheme,
    path_to_string,
    resolve_bot_annotation_parents,
    same_values
)
from nacsos_data.util.errors import NotFoundError, InvalidFilterError

logger = logging.getLogger('nacsos_data.util.annotations.resolve')


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
async def read_item_annotations(session: AsyncSession,
                                filters: AnnotationFilterObject,
                                ignore_hierarchy: bool = False,
                                ignore_repeat: bool = False) -> list[ItemAnnotation]:
    """
    asd
    :param session: Connection to the database
    :param filters:
    :param ignore_hierarchy: if False, looking at keys linearly (ignoring parents)
    :param ignore_repeat: if False, the order is ignored and e.g. single-choice with secondary category
                           virtually becomes multi-choice of two categories
    :return: dictionary (keys are item_ids) of all annotations per item that match the filters.
    """
    filter_join, filter_where, filter_data = filters.get_subquery()

    repeat = 'a.repeat'
    if ignore_repeat:  # if repeat is ignored, always forcing it to 1
        repeat = '1'
    if ignore_hierarchy:
        stmt = text(f'''
            SELECT array_to_json(ARRAY[(a.key, {repeat})::annotation_label]) as label, 
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
async def read_bot_annotations(session: AsyncSession,
                               bot_annotation_metadata_id: str) -> list[BotItemAnnotation]:
    stmt = text(f'''
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
    ''')

    res = (await session.execute(stmt, {
        'bot_annotation_metadata_id': bot_annotation_metadata_id
    })).mappings().all()
    ret = []
    for r in res:
        path = [Label.model_validate(label) for label in r['label']]
        ret.append(BotItemAnnotation(**{**r, 'path': path}))
    return ret


@ensure_session_async
async def read_scopes_for_scheme(session: AsyncSession, scheme_id: str | uuid.UUID, order_by_name: bool = False) \
        -> list[str]:
    """

    :param scheme_id:
    :param session:
    :param order_by_name: if true, will order by name, otherwise by creation date.
    :return:
    """
    stmt = select(AssignmentScope.assignment_scope_id).where(AssignmentScope.annotation_scheme_id == scheme_id)
    if order_by_name:
        stmt = stmt.order_by(AssignmentScope.name)
    else:
        stmt = stmt.order_by(AssignmentScope.time_created)
    return [str(scope_id) for scope_id in (await session.execute(stmt)).scalars().all()]


@ensure_session_async
async def read_annotators(session: AsyncSession, filters: AnnotationFilterObject) -> list[UserModel]:
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
async def read_users(session: AsyncSession, user_ids: list[str] | None) -> list[UserModel]:
    if user_ids is None:
        raise AttributeError('No users specified...')
    stmt = select(User).where(User.user_id.in_(user_ids))
    rslt = (await session.execute(stmt)).mappings().all()
    return [UserModel(**u.__dict__) for u in rslt]


@ensure_session_async
async def read_labels(session: AsyncSession,
                      filters: AnnotationFilterObject,
                      ignore_hierarchy: bool = True,
                      ignore_repeat: bool = True) -> list[list[Label]]:
    # list of all (unique) labels in this selection
    repeat = 'a.repeat'
    if ignore_repeat:  # if repeat is ignored, always forcing it to 1
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
async def get_ordering(session: AsyncSession, scope_id: str | uuid.UUID) -> list[OrderingEntry]:
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

    return [OrderingEntry(**{**r, 'key': f'{scope_id}|{r.item_id}', 'scope_id': str(scope_id)}) for r in res]


@ensure_session_async
async def _get_aux_data(session: AsyncSession,
                        filters: AnnotationFilterObject,
                        ignore_hierarchy: bool = False,
                        ignore_repeat: bool = False) \
        -> tuple[
            AnnotationSchemeModel, list[FlatLabel], list[UserModel], AssignmentMap,
            list[ItemAnnotation], list[OrderingEntry]]:
    scheme: AnnotationSchemeModel | None = await read_annotation_scheme(annotation_scheme_id=filters.scheme_id,
                                                                        session=session)
    if not scheme:
        raise NotFoundError(f'No annotation scheme for {filters.scheme_id}')
    scopes = await read_scopes_for_scheme(scheme_id=filters.scheme_id, session=session)

    annotators = await read_users(user_ids=filters.user_ids, session=session)
    labels = labels_from_scheme(scheme,
                                ignore_hierarchy=ignore_hierarchy,
                                ignore_repeat=ignore_repeat,
                                keys=filters.keys,
                                repeats=filters.repeats)
    if filters.scope_ids is None:
        filter_scopes = scopes
    else:
        # doing it like this to retain the proper order of scopes
        filter_scopes = [scope for scope in scopes if scope in filters.scope_ids]

    item_order: list[OrderingEntry] = []
    for scope_id in filter_scopes:
        item_order += await get_ordering(scope_id=scope_id, session=session)

    annotations: list[ItemAnnotation] = await read_item_annotations(session=session,
                                                                    filters=filters,
                                                                    ignore_hierarchy=ignore_hierarchy,
                                                                    ignore_repeat=ignore_repeat)
    logger.debug(f'Got {len(labels)} labels, {len(annotators)} annotators, {len(item_order):,} items '
                 f'and {len(annotations):,} annotations.')
    assignments: AssignmentMap = {
        str(ass.assignment_id): (ass, order_entry)
        for order_entry in item_order
        for ass in order_entry.assignments
    }
    return scheme, labels, annotators, assignments, annotations, item_order


def _empty_matrix(item_order: list[OrderingEntry], labels: list[FlatLabel]) -> ResolutionMatrix:
    annotation_map: ResolutionMatrix = {}
    for order_entry in item_order:
        annotation_map[order_entry.key] = {}
        for label in labels:
            annotation_map[order_entry.key][label.path_key] = ResolutionCell(
                labels={},
                resolution=BotAnnotationModel(bot_annotation_id=str(uuid.uuid4()),
                                              item_id=order_entry.item_id, order=order_entry.identifier,
                                              key=label.key, repeat=label.repeat),
                status=ResolutionStatus.NEW)
    return annotation_map


def _populate_matrix_annotations(annotation_map: ResolutionMatrix,
                                 assignments: AssignmentMap,
                                 annotations: list[ItemAnnotation]) -> ResolutionMatrix:
    for annotation in annotations:
        assignment, order_entry = assignments[str(annotation.assignment_id)]
        row_key = order_entry.key
        col_key = path_to_string(annotation.path)
        if row_key not in annotation_map or col_key not in annotation_map[row_key]:
            logger.warning(f'Ignoring potentially dangerous incoherent '
                           f'labels during resolution ({row_key} -> {col_key})')
            continue

        user_id = str(annotation.user_id)
        if user_id not in annotation_map[row_key][col_key].labels:
            annotation_map[row_key][col_key].labels[user_id] = []
        entry = ResolutionUserEntry(annotation=annotation)
        if str(annotation.assignment_id) is not None and str(annotation.assignment_id) in assignments:
            entry.assignment = assignment
        annotation_map[row_key][col_key].labels[user_id].append(entry)
    return annotation_map


@ensure_session_async
async def get_resolved_item_annotations(session: AsyncSession,
                                        strategy: ResolutionMethod,
                                        filters: AnnotationFilterObject,
                                        ignore_hierarchy: bool = False,
                                        ignore_repeat: bool = False,
                                        include_empty: bool = True,
                                        bot_meta: BotAnnotationResolution | None = None,
                                        include_new: bool = False,
                                        update_existing: bool = False) -> ResolutionProposal:
    """
    This method retrieves all annotations that match the selected filters and constructs a matrix
    of annotations per item (rows) and label (columns).
    The "cells" contain annotations by each user and the resolution.
    The method also returns associated data

    :param strategy: Resolution strategy (e.g. majority vote of available user annotations)
    :param filters: see `AnnotationFilterObject`
    :param session:
    :param ignore_hierarchy: When True, this will respect the nested nature of the annotation scheme
    :param ignore_repeat: When True, this will ignore the order (`repeat`, e.g. primary, secondary,...) of annotations
    :param include_empty: Should items without annotations be included?
    :param bot_meta: Link to existing resolution
    :param include_new: When `existing_resolution` is set, should I include new items?
    :param update_existing: When `existing_resolution` is set, should I update existing resolutions?
    :return:
    """
    logger.debug(f'Fetching all annotations matching filters: {filters} '
                 f'with ignore_hierarchy={ignore_hierarchy} and ignore_repeat={ignore_repeat}.')
    scheme: AnnotationSchemeModel
    labels: list[FlatLabel]
    annotators: list[UserModel]
    assignments: AssignmentMap
    annotations: list[ItemAnnotation]
    item_order: list[OrderingEntry]
    scheme, labels, annotators, assignments, annotations, item_order = await _get_aux_data(
        filters=filters,
        ignore_hierarchy=ignore_hierarchy,
        ignore_repeat=ignore_repeat,
        session=session)
    label_map = {l.path_key: l for l in labels}
    logger.debug(f'Got {len(labels)} labels, {len(annotators)} annotators, {len(item_order):,} items '
                 f'and {len(annotations):,} annotations.')

    annotation_map: ResolutionMatrix = _empty_matrix(item_order=item_order, labels=labels)
    annotation_map = _populate_matrix_annotations(annotation_map, assignments=assignments, annotations=annotations)

    if bot_meta is not None:
        # Fetch existing bot annotations (resolutions)
        bot_annotations: list[BotItemAnnotation] = await read_bot_annotations(
            bot_annotation_metadata_id=str(bot_meta.bot_annotation_metadata_id),
            session=session
        )
        bot_annotations_map = {str(ba.bot_annotation_id): ba for ba in bot_annotations}
        # Populate existing bot annotations (resolutions)
        # Note: we trust that bot_meta and bot_annotations are in sync and ignore all inconsistencies
        for r_entry in bot_meta.meta.resolutions:
            if (r_entry.ba_id in bot_annotations_map
                    and r_entry.order_key in annotation_map
                    and r_entry.path_key in annotation_map[r_entry.order_key]):
                annotation_map[r_entry.order_key][r_entry.path_key].resolution = bot_annotations_map[r_entry.ba_id]
                # We mark it as unchanged here, this might be updated later
                annotation_map[r_entry.order_key][r_entry.path_key].status = ResolutionStatus.UNCHANGED

        # Compare old and current user annotations
        for entry in bot_meta.meta.snapshot:
            # Note: We are not handling the case where a previously existing annotation is now gone
            if (entry.order_key in annotation_map
                    and entry.path_key in annotation_map[entry.order_key]
                    and entry.user_id in annotation_map[entry.order_key][entry.path_key].labels):
                # Note: It might be, that the first old would match the second new annotation,
                #       then the comparison is not aligned and wrong. More than one annotation
                #       per user and item is rare, so we ignore the issue for simplicity.
                for ai, elem in enumerate(annotation_map[entry.order_key][entry.path_key].labels[entry.user_id]):
                    anno = elem.annotation
                    if anno is not None:
                        anno.old = AnnotationValue.model_validate(entry)
                        # Note: The NEW case is the default, so we only need to set the other two cases
                        if same_values(entry, anno):
                            elem.status = ResolutionStatus.CHANGED
                        else:
                            elem.status = ResolutionStatus.UNCHANGED

        # Drop items that were not in the previous resolution
        if not include_new:
            known_keys = set([entry.order_key for entry in bot_meta.meta.snapshot])
            current_keys = set(annotation_map.keys())
            drop_keys = current_keys - known_keys
            for key in drop_keys:
                del annotation_map[key]
            item_order = [io for io in item_order if io.key not in drop_keys]

    if bot_meta is None or (bot_meta is not None and update_existing):
        # FIXME: new items in an existing resolution are currently not resolved
        if strategy == 'majority':
            annotation_map = naive_majority_vote(annotation_map, label_map, fix_parent_references=False)
        else:
            raise NotImplementedError(f'Resolution strategy "{strategy}" not implemented (yet)!')

    resolve_bot_annotation_parents(annotation_map, label_map)

    # If requested, drop items without annotation from the matrix and the order
    if not include_empty:
        items_with_annotation = set([str(anno.item_id) for anno in annotations])
        item_ids = set([str(o.item_id) for o in item_order])
        empty_items = item_ids - items_with_annotation
        for item_id in empty_items:
            del annotation_map[item_id]
        item_order = [o for o in item_order if str(o.item_id) in items_with_annotation]

    return ResolutionProposal(
        scheme_info=AnnotationSchemeInfo(**scheme.model_dump()),
        labels=labels,
        annotators=annotators,
        ordering=[ResolutionOrdering(**o.model_dump()) for o in item_order],
        matrix=annotation_map
    )
