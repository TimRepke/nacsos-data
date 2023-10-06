import asyncio
import uuid
import json
import logging
from collections import OrderedDict

from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func as F, desc, text, case, distinct, and_, or_, union, literal
from nacsos_data.db.crud.annotations import (
    read_assignment,
    read_assignments_for_scope,
    read_assignments_for_scope_for_user,
    read_assignment_scopes_for_project,
    read_assignment_scopes_for_project_for_user,
    read_annotations_for_assignment,
    read_next_assignment_for_scope_for_user,
    read_next_open_assignment_for_scope_for_user,
    read_annotation_schemes_for_project,
    upsert_annotations,
    read_assignment_scope,
    upsert_annotation_scheme,
    delete_annotation_scheme,
    upsert_assignment_scope,
    delete_assignment_scope,
    read_item_ids_with_assignment_count_for_project,
    read_assignment_counts_for_scope,
    ItemWithCount,
    AssignmentCounts,
    UserProjectAssignmentScope,
    store_assignments,
    store_resolved_bot_annotations,
    update_resolved_bot_annotations,
    read_assignment_overview_for_scope,
    AssignmentScopeEntry,
    read_resolved_bot_annotations,
    read_resolved_bot_annotation_meta,
    read_resolved_bot_annotations_for_meta
)
from nacsos_data.util.annotations.resolve import (
    get_resolved_item_annotations,
    read_annotation_scheme
)
from nacsos_data.models.bot_annotations import (
    AnnotationFilters,
    BotMetaResolve,
    ResolutionMethod,
    BotKind,
    BotAnnotationResolution,
    ResolutionProposal,
    ResolutionMatrix
)

from nacsos_data.db.engine import ensure_session_async
from nacsos_data.db.schemas import AssignmentScope, User, AnnotationScheme
from nacsos_data.models.annotations import (
    AnnotationSchemeModel,
    AnnotationSchemeInfo,
    AnnotationValue,
    ItemAnnotation,
    FlatLabel,
    Label
)
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
    AssignmentMap
)
from nacsos_data.models.users import UserModel
from nacsos_data.util.annotations import (
    AnnotationFilterObject,
    read_item_annotations,
    read_bot_annotations,
    get_ordering
)
from nacsos_data.util.annotations.resolve import _populate_matrix_annotations, _empty_matrix, _get_aux_data
from nacsos_data.util.annotations.resolve.majority_vote import naive_majority_vote
from nacsos_data.util.annotations.validation import (
    resolve_bot_annotation_parents,
    labels_from_scheme,
    path_to_string,
    same_values, has_values
)
from nacsos_data.util.annotations import AnnotationFilterObject, dehydrate_user_annotations, dehydrate_resolutions
from nacsos_data.models.annotations import AssignmentStatus
from nacsos_data.db.schemas import AnnotationScheme, AssignmentScope, Annotation, User, Project, BotAnnotationMetaData, \
    BotAnnotation, Assignment, AcademicItem
from nacsos_data.db import get_engine_async

db_engine = get_engine_async(conf_file='../nacsos-core/config/remote.env')

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=logging.INFO)
logger = logging.getLogger('migrate')
logger.setLevel(logging.DEBUG)

# This script translates the old groupedcollection-style botannotation to the new matrix-style meta-data


async def guess_scopes(bas: list[BotItemAnnotation], scheme_id: str, session: AsyncSession):
    stmt = text('''SELECT DISTINCT ass.assignment_scope_id, scp.name
                   FROM assignment ass
                   JOIN assignment_scope scp ON ass.assignment_scope_id = scp.assignment_scope_id
                   WHERE ass.item_id = :item_id AND ass.annotation_scheme_id = :scheme_id;''')

    item_ids = set([str(ba.item_id) for ba in bas])
    scope_ids = {}
    for iid in item_ids:
        rslt = (await session.execute(stmt, {'item_id': iid, 'scheme_id': scheme_id})).mappings().all()
        for r in rslt:
            sid = str(r['assignment_scope_id'])
            if sid not in scope_ids:
                scope_ids[sid] = {'scope_id': sid, 'name': r['name']}
    return list(scope_ids.values())


async def main():
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(BotAnnotationMetaData)

        rslt = (await session.execute(stmt)).scalars().all()

        for ba in rslt:
            stmtp = select(Project).where(Project.project_id == ba.project_id)
            # stmt = text('SELECT * FROM bot_annotation_metadata;')
            project = (await session.execute(stmtp)).scalars().one_or_none()

            if ba.meta is not None:
                try:
                    # print(json.dumps(ba.meta, indent=2))
                    logger.info(f'Handling "{ba.name}" ({ba.bot_annotation_metadata_id}) '
                                f'in project "{project.name}" ({project.project_id})')
                    ignore_repeat = ba.meta['ignore_repeat']
                    ignore_hierarchy = ba.meta['ignore_hierarchy']
                    filters = AnnotationFilterObject.model_validate(ba.meta['filters'])

                    if filters.scheme_id != str(ba.annotation_scheme_id):
                        logger.debug(f'scheme_id {filters.scheme_id} to {str(ba.annotation_scheme_id)}')
                        filters.scheme_id = str(ba.annotation_scheme_id)

                    bot_annotations: list[BotItemAnnotation] = await read_bot_annotations(
                        bot_annotation_metadata_id=str(ba.bot_annotation_metadata_id),
                        session=session
                    )
                    item_ids_ba = set([str(ba.item_id) for ba in bot_annotations])

                    scheme, labels, annotators, assignments, annotations, item_order = await _get_aux_data(
                        filters=filters,
                        ignore_hierarchy=ignore_hierarchy,
                        ignore_repeat=ignore_repeat,
                        session=session)
                    logger.debug(f'Got {len(labels)} labels, {len(annotators)} annotators, {len(item_order):,} items '
                                 f'and {len(annotations):,} annotations and {len(bot_annotations)} bot annotations.')

                    annotation_map: ResolutionMatrix = _empty_matrix(item_order=item_order, labels=labels)
                    id2am = {r.split('|')[1]: r for r in annotation_map.keys()}

                    annotation_map = _populate_matrix_annotations(annotation_map, assignments=assignments,
                                                                  annotations=annotations)

                    if len(bot_annotations) > 0:
                        continue

                        for bai in bot_annotations:
                            path_key = path_to_string(bai.path)
                            row_key = id2am[str(bai.item_id)]
                            annotation_map[row_key][path_key].resolution = bai
                            annotation_map[row_key][path_key].status = ResolutionStatus.UNCHANGED

                        item_ids_or = set([str(io.item_id) for io in item_order])
                        to_drop = item_ids_or - item_ids_ba
                        logger.info(f'Dropping {len(to_drop)} items from the matrix')
                        for iid in to_drop:
                            del annotation_map[id2am[iid]]
                        # item_order = [o for o in item_order if str(o.item_id) in to_drop]

                        snapshot = dehydrate_user_annotations(annotation_map)
                        resolutions = dehydrate_resolutions(annotation_map)
                        meta = BotMetaResolve(algorithm='majority',
                                              filters=filters,
                                              ignore_hierarchy=bool(ignore_hierarchy),
                                              ignore_repeat=bool(ignore_repeat),
                                              snapshot=snapshot,
                                              resolutions=resolutions)

                        ba.meta = meta.model_dump()
                        await session.flush()
                    else:
                        # annotation_map
                        # session.add(BotAnnotation())
                        #
                        # has_values
                        logger.warning('SKIPPING FOR NOW!')

                except Exception as e:
                    logger.exception(e)
                    logger.info(ba.name)
                    raise e


if __name__ == '__main__':
    asyncio.run(main())
