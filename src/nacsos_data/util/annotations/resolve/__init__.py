import logging
import uuid
from sqlalchemy import text, select

from nacsos_data.db.engine import ensure_session_async, DBSession
from nacsos_data.db.schemas import AssignmentScope, User, AnnotationScheme
from nacsos_data.models.annotations import AnnotationSchemeModel, AnnotationSchemeInfo, AnnotationValue, ItemAnnotation, FlatLabel, Label
from nacsos_data.models.bot_annotations import (
    BotAnnotationResolution,
    ResolutionUserEntry,
    ResolutionOrdering,
    ResolutionProposal,
    BotAnnotationModel,
    BotItemAnnotation,
    ResolutionMatrix,
    ResolutionMethod,
    ResolutionStatus,
    ResolutionCell,
    OrderingEntry,
    AssignmentMap,
)
from nacsos_data.models.users import UserModel
from nacsos_data.util.annotations import read_item_annotations, read_bot_annotations, get_ordering
from nacsos_data.util.annotations.resolve.majority_vote import naive_majority_vote
from nacsos_data.util.annotations.validation import resolve_bot_annotation_parents, labels_from_scheme, path_to_string, same_values
from nacsos_data.util.errors import NotFoundError

logger = logging.getLogger('nacsos_data.util.annotations.resolve')


@ensure_session_async
async def read_labels(session: DBSession, assignment_scope_id: str | uuid.UUID, ignore_hierarchy: bool = True, ignore_repeat: bool = True) -> list[list[Label]]:
    """
    Read all labels from the existing annotations that fit these filters.
    :param session:
    :param assignment_scope_id:
    :param ignore_hierarchy:
    :param ignore_repeat:
    :return:
    """
    # list of all (unique) labels in this selection
    repeat = 'a.repeat'
    if ignore_repeat:  # if repeat is ignored, always forcing it to 1
        repeat = '1'
    if ignore_hierarchy:
        return [
            [Label.model_validate(sub_label) for sub_label in label]
            for label in (
                await session.execute(
                    text(f"""
                    SELECT array_to_json(label) as label
                    FROM (
                    SELECT DISTINCT ARRAY[(a.key, {repeat})::annotation_label] as label
                    FROM annotation AS a
                            JOIN assignment ass ON a.assignment_id = ass.assignment_id
                    WHERE ass.assignment_scope_id = :scope_id;
                    """),
                    {'scope_id': assignment_scope_id},
                )
            ).scalars()
        ]
    else:
        return [
            [Label.model_validate(sub_label) for sub_label in label]
            for label in (
                await session.execute(
                    text(f"""
                    SELECT array_to_json(label) as label
                    FROM (
                        WITH RECURSIVE ctename AS (
                            SELECT a.annotation_id, a.parent, ARRAY[(a.key, {repeat})::annotation_label] as label
                            FROM annotation AS a
                                    JOIN assignment ass ON a.assignment_id = ass.assignment_id
                            WHERE ass.assignment_scope_id = :scope_id

                            UNION ALL

                            SELECT a.annotation_id, a.parent,
                                   array_append(ctename.label, ((a.key, {repeat})::annotation_label))
                            FROM annotation a
                                    JOIN ctename ON a.annotation_id = ctename.parent
                        )
                        SELECT DISTINCT label
                        FROM ctename
                        WHERE parent is NULL
                    ) labels;"""),
                    {'scope_id': assignment_scope_id},
                )
            ).scalars()
        ]


@ensure_session_async
async def read_scopes_for_scheme(session: DBSession, scheme_id: str | uuid.UUID, order_by_name: bool = False) -> list[str]:
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
async def read_annotation_scheme(session: DBSession, annotation_scheme_id: str | uuid.UUID) -> AnnotationSchemeModel | None:
    stmt = select(AnnotationScheme).filter_by(annotation_scheme_id=annotation_scheme_id)
    result = (await session.execute(stmt)).scalars().one_or_none()
    if result is not None:
        return AnnotationSchemeModel(**result.__dict__)
    return None


@ensure_session_async
async def read_annotation_scheme_for_scope(session: DBSession, assignment_scope_id: str | uuid.UUID) -> AnnotationSchemeModel | None:
    stmt = (
        select(AnnotationScheme)
        .join(AssignmentScope, AssignmentScope.annotation_scheme_id == AnnotationScheme.annotation_scheme_id)
        .where(AssignmentScope.assignment_scope_id == assignment_scope_id)
    )
    result = (await session.execute(stmt)).scalars().one_or_none()
    if result is not None:
        return AnnotationSchemeModel(**result.__dict__)
    return None


@ensure_session_async
async def read_annotators(session: DBSession, assignment_scope_id: str | uuid.UUID) -> list[UserModel]:
    # list of all (unique) users that have at least one annotation in the set
    return [
        UserModel.model_validate(user)
        for user in (
            await session.execute(
                text(
                    'SELECT DISTINCT u.* '
                    'FROM annotation AS a '
                    '   JOIN assignment ass ON a.assignment_id = ass.assignment_id '
                    '    JOIN "user" u on u.user_id = a.user_id '
                    'WHERE ass.assignment_scope_id = :scope_id;'
                ),
                {'scope_id': assignment_scope_id},
            )
        )
        .mappings()
        .all()
    ]


@ensure_session_async
async def read_users(session: DBSession, user_ids: list[str] | None) -> list[UserModel]:
    if user_ids is None:
        raise AttributeError('No users specified...')
    stmt = select(User).where(User.user_id.in_(user_ids))
    rslt = (await session.execute(stmt)).scalars().all()
    return [UserModel.model_validate(u.__dict__) for u in rslt]


@ensure_session_async
async def _get_aux_data(
    session: DBSession, assignment_scope_id: str | uuid.UUID, ignore_hierarchy: bool = False, ignore_repeat: bool = False
) -> tuple[AnnotationSchemeModel, list[FlatLabel], list[UserModel], AssignmentMap, list[ItemAnnotation], list[OrderingEntry]]:
    scheme: AnnotationSchemeModel | None = await read_annotation_scheme_for_scope(assignment_scope_id=assignment_scope_id, session=session)
    if not scheme:
        raise NotFoundError(f'No annotation scheme for scope {assignment_scope_id}')

    annotators = await read_annotators(assignment_scope_id=assignment_scope_id, session=session)
    labels = labels_from_scheme(scheme, ignore_hierarchy=ignore_hierarchy, ignore_repeat=ignore_repeat)

    item_order: list[OrderingEntry] = await get_ordering(assignment_scope_id=assignment_scope_id, session=session)

    annotations: list[ItemAnnotation] = await read_item_annotations(
        session=session, assignment_scope_id=assignment_scope_id, ignore_hierarchy=ignore_hierarchy, ignore_repeat=ignore_repeat
    )
    logger.debug(f'Got {len(labels)} labels, {len(annotators)} annotators, {len(item_order):,} items and {len(annotations):,} annotations.')
    assignments: AssignmentMap = {str(ass.assignment_id): (ass, order_entry) for order_entry in item_order for ass in order_entry.assignments}
    return scheme, labels, annotators, assignments, annotations, item_order


def _empty_matrix(item_order: list[OrderingEntry], labels: list[FlatLabel]) -> ResolutionMatrix:
    annotation_map: ResolutionMatrix = {}
    for order_entry in item_order:
        annotation_map[order_entry.item_id] = {}
        for label in labels:
            annotation_map[order_entry.item_id][label.path_key] = ResolutionCell(
                labels={},
                resolution=BotAnnotationModel(
                    bot_annotation_id=str(uuid.uuid4()), item_id=order_entry.item_id, order=order_entry.identifier, key=label.key, repeat=label.repeat
                ),
                status=ResolutionStatus.NEW,
            )
    return annotation_map


def _populate_matrix_annotations(annotation_map: ResolutionMatrix, assignments: AssignmentMap, annotations: list[ItemAnnotation]) -> ResolutionMatrix:
    for annotation in annotations:
        assignment, order_entry = assignments[str(annotation.assignment_id)]
        row_key = order_entry.item_id
        col_key = path_to_string(annotation.path)
        if row_key not in annotation_map or col_key not in annotation_map[row_key]:
            logger.warning(f'Ignoring potentially dangerous incoherent labels during resolution ({row_key} -> {col_key})')
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
async def get_annotation_matrix(
    session: DBSession, assignment_scope_id: str | uuid.UUID, ignore_hierarchy: bool = False, ignore_repeat: bool = False
) -> tuple[AnnotationSchemeModel, list[FlatLabel], list[UserModel], AssignmentMap, list[ItemAnnotation], list[OrderingEntry], ResolutionMatrix]:
    scheme: AnnotationSchemeModel
    labels: list[FlatLabel]
    annotators: list[UserModel]
    assignments: AssignmentMap
    annotations: list[ItemAnnotation]
    item_order: list[OrderingEntry]
    scheme, labels, annotators, assignments, annotations, item_order = await _get_aux_data(
        assignment_scope_id=assignment_scope_id, ignore_hierarchy=ignore_hierarchy, ignore_repeat=ignore_repeat, session=session
    )

    annotation_map: ResolutionMatrix = _empty_matrix(item_order=item_order, labels=labels)

    # FIXME: in the update_existing=False case, we are still loading this (and we may want to wait for existing data)
    annotation_map = _populate_matrix_annotations(annotation_map, assignments=assignments, annotations=annotations)
    return scheme, labels, annotators, assignments, annotations, item_order, annotation_map


@ensure_session_async
async def get_resolved_item_annotations(  # noqa: C901
    session: DBSession,
    strategy: ResolutionMethod,
    assignment_scope_id: str | uuid.UUID,
    ignore_hierarchy: bool = False,
    ignore_repeat: bool = False,
    include_empty: bool = True,
    bot_meta: BotAnnotationResolution | None = None,
    include_new: bool = False,
    update_existing: bool = False,
) -> ResolutionProposal:
    """
    This method retrieves all annotations that match the selected filters and constructs a matrix
    of annotations per item (rows) and label (columns).
    The "cells" contain annotations by each user and the resolution.
    The method also returns associated data

    :param strategy: Resolution strategy (e.g. majority vote of available user annotations)
    :param assignment_scope_id:
    :param session:
    :param ignore_hierarchy: When True, this will respect the nested nature of the annotation scheme
    :param ignore_repeat: When True, this will ignore the order (`repeat`, e.g. primary, secondary,...) of annotations
    :param include_empty: Should items without annotations be included?
    :param bot_meta: Link to existing resolution
    :param include_new: When `existing_resolution` is set, should I include new items?
    :param update_existing: When `existing_resolution` is set, should I update existing resolutions?
    :return:
    """
    logger.debug(f'Fetching all annotations in scope {assignment_scope_id} with ignore_hierarchy={ignore_hierarchy} and ignore_repeat={ignore_repeat}.')
    scheme: AnnotationSchemeModel
    labels: list[FlatLabel]
    annotators: list[UserModel]
    assignments: AssignmentMap
    annotations: list[ItemAnnotation]
    item_order: list[OrderingEntry]
    annotation_map: ResolutionMatrix
    scheme, labels, annotators, assignments, annotations, item_order, annotation_map = await get_annotation_matrix(
        assignment_scope_id=assignment_scope_id, ignore_hierarchy=ignore_hierarchy, ignore_repeat=ignore_repeat, session=session
    )
    label_map = {l.path_key: l for l in labels}

    if bot_meta is not None:
        # Fetch existing bot annotations (resolutions)
        bot_annotations: list[BotItemAnnotation] = await read_bot_annotations(
            bot_annotation_metadata_id=str(bot_meta.bot_annotation_metadata_id), session=session
        )
        bot_annotations_map = {str(ba.bot_annotation_id): ba for ba in bot_annotations}

        logger.debug(f'Found {len(bot_annotations_map):,} existing BotAnnotations in for the key and I remembered {len(bot_meta.meta.resolutions):,} of those.')

        if len(bot_meta.meta.resolutions) > 0:
            # Populate existing bot annotations (resolutions)
            # Note: we trust that bot_meta and bot_annotations are in sync and ignore all inconsistencies
            for r_entry in bot_meta.meta.resolutions:
                if r_entry.ba_id in bot_annotations_map and r_entry.order_key in annotation_map and r_entry.path_key in annotation_map[r_entry.order_key]:
                    annotation_map[r_entry.order_key][r_entry.path_key].resolution = bot_annotations_map[r_entry.ba_id]
                    # We mark it as unchanged here, this might be updated later
                    annotation_map[r_entry.order_key][r_entry.path_key].status = ResolutionStatus.UNCHANGED
        else:
            # This is a bodge, we are assuming that empty resultion list means a "reset" and just go with the flow
            logger.debug('Bodging assignment of existing resolutions')
            for ba in bot_annotations:
                path_key = path_to_string(ba.path)
                if str(ba.item_id) in annotation_map and path_key in annotation_map[str(ba.item_id)]:
                    annotation_map[str(ba.item_id)][path_key].resolution = ba

        # Compare old and current user annotations
        for entry in bot_meta.meta.snapshot:
            # Note: We are not handling the case where a previously existing annotation is now gone
            if (
                entry.order_key in annotation_map
                and entry.path_key in annotation_map[entry.order_key]
                and entry.user_id in annotation_map[entry.order_key][entry.path_key].labels
            ):
                # Note: It might be, that the first old would match the second new annotation,
                #       then the comparison is not aligned and wrong. More than one annotation
                #       per user and item is rare, so we ignore the issue for simplicity.
                for _ai, elem in enumerate(annotation_map[entry.order_key][entry.path_key].labels[entry.user_id]):
                    anno = elem.annotation
                    if anno is not None:
                        anno.old = AnnotationValue.model_validate(entry)
                        # Note: The NEW case is the default, so we only need to set the other two cases
                        if same_values(entry, anno):
                            elem.status = ResolutionStatus.UNCHANGED
                        else:
                            elem.status = ResolutionStatus.CHANGED

        # Drop items that were not in the previous resolution
        if not include_new and len(bot_meta.meta.snapshot) > 0:
            known_keys = {entry.item_id for entry in bot_meta.meta.snapshot}  # FIXME: used to be entry.order_key
            current_keys = set(annotation_map.keys())
            drop_keys = current_keys - known_keys
            for key in drop_keys:
                del annotation_map[key]
            item_order = [io for io in item_order if io.item_id not in drop_keys]
    else:
        logger.debug('Did not receive bot_meta.')

    if bot_meta is None or (bot_meta is not None and update_existing):
        # FIXME: new items in an existing resolution are currently not resolved
        if strategy == 'majority':
            annotation_map = naive_majority_vote(annotation_map, label_map, fix_parent_references=False)
        else:
            raise NotImplementedError(f'Resolution strategy "{strategy}" not implemented (yet)!')

    resolve_bot_annotation_parents(annotation_map, label_map)

    # If requested, drop items without annotation from the matrix and the order
    if not include_empty:
        items_with_annotation = {str(anno.item_id) for anno in annotations}
        item_id_to_key = {str(o.item_id): o.item_id for o in item_order}
        item_ids = set(item_id_to_key.keys())
        empty_items = item_ids - items_with_annotation

        logger.debug(
            f'Saw {len(items_with_annotation):,} items with annotation, '
            f'{len(item_ids):,} items in the lookup key, and '
            f'removing {len(empty_items):,} items that had no annotation.'
        )

        for item_id in empty_items:
            del annotation_map[item_id_to_key[item_id]]

        item_order = [o for o in item_order if str(o.item_id) not in empty_items]

    return ResolutionProposal(
        scheme_info=AnnotationSchemeInfo(**scheme.model_dump()),
        labels=labels,
        annotators=annotators,
        ordering=[ResolutionOrdering(**o.model_dump()) for o in item_order],
        matrix=annotation_map,
    )
