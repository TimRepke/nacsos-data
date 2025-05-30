import uuid
import logging

from pydantic import BaseModel
from sqlalchemy import select, delete, asc, desc
from sqlalchemy.sql import text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncConnection

from nacsos_data.db.schemas import (
    Annotation,
    AnnotationScheme,
    Assignment,
    AssignmentScope,
    BotAnnotationMetaData,
    BotAnnotation
)
from nacsos_data.models.annotations import (
    AnnotationModel,
    AnnotationSchemeModel,
    AssignmentModel,
    AssignmentScopeModel,
    AssignmentStatus
)
from nacsos_data.models.bot_annotations import (
    BotMetaResolve,
    ResolutionMethod,
    BotKind,
    BotAnnotationModel,
    BotAnnotationResolution,
    ResolutionProposal,
    ResolutionMatrix
)
from nacsos_data.util.annotations.validation import (
    validate_annotated_assignment,
    merge_scheme_and_annotations,
    has_values
)

from . import upsert_orm
from ..engine import ensure_session_async, DBSession, DatabaseEngineAsync, ensure_connection_async
from ...util.annotations import (
    dehydrate_user_annotations,
    dehydrate_resolutions
)
from ...util.annotations.resolve import get_resolved_item_annotations
from ...util.errors import NotFoundError, MissingIdError

logger = logging.getLogger('nacsos_data.crud.annotations')


class UserProjectAssignmentScope(BaseModel):
    scope: AssignmentScopeModel
    scheme_name: str
    scheme_description: str
    num_assignments: int
    num_open: int
    num_partial: int
    num_completed: int


@ensure_session_async
async def read_assignment_scopes_for_project_for_user(session: DBSession,
                                                      project_id: str | uuid.UUID,
                                                      user_id: str | uuid.UUID) \
        -> list[UserProjectAssignmentScope]:
    stmt = text("""
    SELECT scope.*,
           scheme.name AS scheme_name,
           scheme.description AS scheme_description,
           COUNT(DISTINCT assi.assignment_id) AS num_assignments,
           SUM(CASE WHEN assi.status = 'OPEN' THEN 1 ELSE 0 END) AS num_open,
           SUM(CASE WHEN assi.status = 'PARTIAL' THEN 1 ELSE 0 END) AS num_partial,
           SUM(CASE WHEN assi.status = 'FULL' THEN 1 ELSE 0 END) AS num_completed
    FROM assignment_scope scope
             FULL OUTER JOIN annotation_scheme scheme ON scope.annotation_scheme_id = scheme.annotation_scheme_id
             LEFT OUTER JOIN assignment assi ON scope.assignment_scope_id = assi.assignment_scope_id
    WHERE assi.user_id = :user_id AND
          scheme.project_id = :project_id
    GROUP BY scope.assignment_scope_id, scheme_name, scheme_description, scope.time_created
    ORDER BY scope.time_created;
    """)
    result = await session.execute(stmt, {'user_id': user_id, 'project_id': project_id})
    result_list = result.mappings().all()
    return [
        UserProjectAssignmentScope(
            scope=AssignmentScopeModel(
                assignment_scope_id=res['assignment_scope_id'],
                annotation_scheme_id=res['annotation_scheme_id'],
                name=res['name'],
                description=res['description'],
                time_created=res['time_created']
            ),
            scheme_name=res['scheme_name'],
            scheme_description=res['scheme_description'],
            num_assignments=res['num_assignments'],
            num_open=res['num_open'],
            num_partial=res['num_partial'],
            num_completed=res['num_completed'],
        )
        for res in result_list
    ]


@ensure_session_async
async def read_assignment_scopes_for_project(session: DBSession,
                                             project_id: str | uuid.UUID) -> list[AssignmentScopeModel]:
    result = (
        await session.execute(
            select(AssignmentScope)
            .join(AnnotationScheme,
                  AnnotationScheme.annotation_scheme_id == AssignmentScope.annotation_scheme_id)
            .where(AnnotationScheme.project_id == project_id)
            .order_by(desc(AssignmentScope.time_created))
        )
    ).scalars().all()
    return [AssignmentScopeModel(**res.__dict__) for res in result]


@ensure_session_async
async def read_assignments_for_scope_for_user(session: DBSession,
                                              assignment_scope_id: str | uuid.UUID,
                                              user_id: str | uuid.UUID,
                                              limit: int | None = None) -> list[AssignmentModel]:
    stmt = (
        select(Assignment)
        .filter_by(assignment_scope_id=assignment_scope_id, user_id=user_id)
        .order_by(asc(Assignment.order))
    )
    if limit is not None:
        stmt = stmt.limit(limit)

    result = (await session.execute(stmt)).scalars().all()
    return [AssignmentModel(**res.__dict__) for res in result]


@ensure_session_async
async def read_assignments_for_scope(session: DBSession,
                                     assignment_scope_id: str | uuid.UUID) -> list[AssignmentModel]:
    result = (
        await session.execute(
            select(Assignment)
            .filter_by(assignment_scope_id=assignment_scope_id)
            .order_by(asc(Assignment.order)))
    ).scalars().all()
    return [AssignmentModel(**res.__dict__) for res in result]


class AssignmentInfoLabel(BaseModel):
    repeat: int
    value_int: int | None = None
    value_bool: bool | None = None
    multi_ind: list[int] | None = None


class AssignmentInfo(BaseModel):
    user_id: str | uuid.UUID
    username: str
    order: int
    assignment_id: str | uuid.UUID
    status: AssignmentStatus
    labels: dict[str, list[AssignmentInfoLabel]] | None = None


class AssignmentScopeEntry(BaseModel):
    item_id: str | uuid.UUID
    first_occurrence: int
    identifier: int
    assignments: list[AssignmentInfo]


@ensure_connection_async
async def read_assignment_overview_for_scope(db_conn: AsyncConnection,
                                             assignment_scope_id: str | uuid.UUID) -> list[AssignmentScopeEntry]:
    result = await db_conn.execute(
        text('''
            WITH labels_pre as (SELECT ass.assignment_id,
                               ann.key,
                               jsonb_agg(jsonb_build_object('repeat', ann.repeat,
                                                            'value_bool', ann.value_bool,
                                                            'value_int', ann.value_int,
                                                            'multi_int', ann.multi_int)) as label
                            FROM assignment ass
                                     LEFT OUTER JOIN annotation ann on ann.assignment_id = ass.assignment_id
                            WHERE ass.assignment_scope_id = :scope_id
                            GROUP BY ass.assignment_id, ann.key),
                 labels as (SELECT assignment_id,
                                   jsonb_object_agg(key, label) filter (where key is not null ) as labels
                            FROM labels_pre
                            GROUP BY assignment_id),
                 assis as (SELECT ass.item_id,
                                  MIN(ass."order") as first_occurrence,
                                  array_agg(jsonb_build_object(
                                          'assignment_id', ass.assignment_id,
                                          'user_id', ass.user_id,
                                          'username', u.username,
                                          'status', ass.status,
                                          'order', ass."order",
                                          'labels', labels.labels
                                      )) as assignments
                           FROM assignment ass
                                    JOIN "user" u on u.user_id = ass.user_id
                                    JOIN labels on labels.assignment_id = ass.assignment_id
                           --WHERE ass.assignment_scope_id = :scope_id
                           GROUP BY ass.item_id
                           ORDER BY first_occurrence)
            SELECT row_number() over () as identifier,
                   assis.*
            FROM assis;'''),
        {'scope_id': assignment_scope_id})
    return [AssignmentScopeEntry.model_validate(r) for r in result.mappings().all()]


async def read_next_assignment_for_scope_for_user(current_assignment_id: str | uuid.UUID,
                                                  assignment_scope_id: str | uuid.UUID,
                                                  user_id: str | uuid.UUID,
                                                  db_engine: DatabaseEngineAsync) -> AssignmentModel | None:
    session: AsyncSession
    async with db_engine.session() as session:
        result = (
            await session.execute(
                text('''
                WITH tmp as (SELECT assignment_id,
                                LEAD(assignment_id, 1) over (order by "order") as next_assignment_id
                         FROM assignment
                         WHERE user_id = :user_id AND assignment_scope_id=:assignment_scope_id)
                SELECT assignment.*
                FROM assignment
                    JOIN tmp  ON  tmp.next_assignment_id=assignment.assignment_id
                WHERE tmp.assignment_id=:assignment_id;'''),
                {'user_id': user_id,
                 'assignment_scope_id': assignment_scope_id,
                 'assignment_id': current_assignment_id})
        ).mappings().one_or_none()
        if result is not None:
            return AssignmentModel(**result)  # type: ignore[misc]

    # Try to fall back on next open assignment (apparently we reached the end of the list here).
    ret: AssignmentModel | None = await read_next_open_assignment_for_scope_for_user(
        assignment_scope_id=assignment_scope_id,
        user_id=user_id,
        db_engine=db_engine)
    return ret


@ensure_session_async
async def read_next_open_assignment_for_scope_for_user(session: DBSession,
                                                       assignment_scope_id: str | uuid.UUID,
                                                       user_id: str | uuid.UUID) -> AssignmentModel | None:
    stmt = select(Assignment) \
        .where(Assignment.user_id == user_id,
               Assignment.assignment_scope_id == assignment_scope_id,
               Assignment.status == 'OPEN') \
        .order_by(asc(Assignment.order)) \
        .limit(1)
    result = (await session.execute(stmt)).scalars().one_or_none()
    if result is None:
        return None
    return AssignmentModel(**result.__dict__)


@ensure_session_async
async def read_assignment(session: DBSession, assignment_id: str | uuid.UUID) -> AssignmentModel | None:
    stmt = select(Assignment).filter_by(assignment_id=assignment_id)
    result = (await session.execute(stmt)).scalars().one_or_none()

    if result is None:
        return None

    return AssignmentModel(**result.__dict__)


@ensure_session_async
async def read_annotations_for_scope_for_user(session: DBSession,
                                              assignment_scope_id: str | uuid.UUID,
                                              user_id: str | uuid.UUID) -> list[AnnotationModel]:
    stmt = (select(Annotation)
            .join(Assignment, Assignment.assignment_id == Annotation.assignment_id)
            .where(Assignment.assignment_scope_id == assignment_scope_id,
                   Assignment.user_id == user_id))
    result = (await session.execute(stmt)).scalars().all()
    return [AnnotationModel(**res.__dict__) for res in result]


@ensure_session_async
async def read_annotations_for_assignment(session: DBSession,
                                          assignment_id: str | uuid.UUID) -> list[AnnotationModel]:
    stmt = select(Annotation).filter_by(assignment_id=assignment_id)
    result = (await session.execute(stmt)).scalars().all()
    return [AnnotationModel(**res.__dict__) for res in result]


@ensure_session_async
async def read_assignment_scope(session: DBSession,
                                assignment_scope_id: str | uuid.UUID) -> AssignmentScopeModel | None:
    stmt = select(AssignmentScope).filter_by(assignment_scope_id=assignment_scope_id)
    result = (await session.execute(stmt)).scalars().one_or_none()
    if result is not None:
        return AssignmentScopeModel(**result.__dict__)
    return None


@ensure_session_async
async def read_annotation_scheme(session: DBSession,
                                 assignment_id: str | uuid.UUID | None = None,
                                 assignment_scope_id: str | uuid.UUID | None = None) -> AnnotationSchemeModel | None:
    if assignment_id is not None:
        stmt = (select(AnnotationScheme)
                .join(Assignment, AnnotationScheme.annotation_scheme_id == Assignment.annotation_scheme_id)
                .where(Assignment.assignment_id == assignment_id))
    elif assignment_scope_id is not None:
        stmt = (select(AnnotationScheme)
                .join(AssignmentScope, AnnotationScheme.annotation_scheme_id == AssignmentScope.annotation_scheme_id)
                .where(AssignmentScope.assignment_scope_id == assignment_scope_id))
    else:
        raise AssertionError('Both, assignment_id and assignment_scope_id are empty.')
    result = (await session.execute(stmt)).scalars().one_or_none()
    if result is not None:
        return AnnotationSchemeModel(**result.__dict__)
    return None


@ensure_session_async
async def read_annotation_scheme_for_assignment(session: DBSession,
                                                assignment_id: str | uuid.UUID) -> AnnotationSchemeModel | None:
    return await read_annotation_scheme(session=session, assignment_id=assignment_id)


@ensure_session_async
async def read_annotation_scheme_for_scope(session: DBSession,
                                           assignment_scope_id: str | uuid.UUID) -> AnnotationSchemeModel | None:
    return await read_annotation_scheme(session=session, assignment_scope_id=assignment_scope_id)


@ensure_session_async
async def read_annotation_schemes_for_project(session: DBSession,
                                              project_id: str | uuid.UUID) -> list[AnnotationSchemeModel]:
    stmt = select(AnnotationScheme).filter_by(project_id=project_id)
    result = (await session.execute(stmt)).scalars().all()
    return [AnnotationSchemeModel(**res.__dict__) for res in result]


async def read_scheme_with_annotations(assignment_id: str | uuid.UUID,
                                       db_engine: DatabaseEngineAsync) -> AnnotationSchemeModel | None:
    annotation_scheme = await read_annotation_scheme_for_assignment(assignment_id=assignment_id, db_engine=db_engine)
    annotations = await read_annotations_for_assignment(assignment_id=assignment_id, db_engine=db_engine)

    if annotation_scheme is not None and annotations is not None:
        annotated_annotation_scheme = merge_scheme_and_annotations(annotation_scheme, annotations)
        return annotated_annotation_scheme
    return None


@ensure_session_async
async def update_assignment_status(session: DBSession,
                                   assignment_id: str | uuid.UUID,
                                   status: AssignmentStatus) -> None:
    stmt = select(Assignment).where(Assignment.assignment_id == assignment_id)
    assignment: Assignment = (await session.scalars(stmt)).one()
    assignment.status = status
    await session.flush_or_commit()


async def upsert_annotation_scheme(annotation_scheme: AnnotationSchemeModel,
                                   db_engine: DatabaseEngineAsync) -> str | uuid.UUID | None:
    key = await upsert_orm(upsert_model=annotation_scheme,
                           Schema=AnnotationScheme,
                           primary_key=AnnotationScheme.annotation_scheme_id.name,
                           db_engine=db_engine,
                           use_commit=True)
    return key


@ensure_session_async
async def delete_annotation_scheme(session: DBSession,
                                   annotation_scheme_id: str | uuid.UUID) -> None:
    stmt = select(AnnotationScheme).filter_by(annotation_scheme_id=annotation_scheme_id)
    annotation_scheme = (await session.scalars(stmt)).one_or_none()
    if annotation_scheme is not None:
        await session.delete(annotation_scheme)
        await session.flush_or_commit()
    else:
        raise ValueError(f'AnnotationScheme with id="{annotation_scheme_id}" does not seem to exist.')


async def upsert_assignment_scope(assignment_scope: AssignmentScopeModel,
                                  db_engine: DatabaseEngineAsync) -> str | uuid.UUID | None:
    key = await upsert_orm(upsert_model=assignment_scope,
                           Schema=AssignmentScope,
                           primary_key=AssignmentScope.assignment_scope_id.name,
                           db_engine=db_engine,
                           use_commit=True)
    return key


@ensure_session_async
async def delete_assignment_scope(session: DBSession,
                                  assignment_scope_id: str | uuid.UUID) -> None:
    stmt = select(AssignmentScope).filter_by(assignment_scope_id=assignment_scope_id)
    assignment_scope = (await session.scalars(stmt)).one_or_none()
    if assignment_scope is not None:
        await session.delete(assignment_scope)
        await session.flush_or_commit()
    else:
        raise ValueError(f'Assignment scope with id="{assignment_scope_id}" does not seem to exist.')


async def upsert_annotations(annotations: list[AnnotationModel],
                             assignment_id: str | uuid.UUID | None,
                             db_engine: DatabaseEngineAsync) -> AssignmentStatus | None:
    if not all([annotation is not None and annotation.annotation_id is not None for annotation in annotations]):
        raise ValueError('One or more annotations have no ID, this an undefined behaviour.')

    if assignment_id is not None:
        existing_annotations = await read_annotations_for_assignment(assignment_id=assignment_id, db_engine=db_engine)
        existing_ids = set([str(annotation.annotation_id) for annotation in existing_annotations])
        submitted_ids = set([str(annotation.annotation_id) for annotation in annotations])

        ids_to_remove = existing_ids - submitted_ids
        ids_to_update = existing_ids - ids_to_remove
        ids_to_create = submitted_ids - ids_to_update
    else:
        ids_to_remove = set()
        ids_to_update = set()
        ids_to_create = set([str(annotation.annotation_id) for annotation in annotations])

    logger.debug(f'[upsert_annotations] CREATING new annotations with ids: {ids_to_create}')
    logger.debug(f'[upsert_annotations] UPDATING existing annotations with ids: {ids_to_update}')
    logger.debug(f'[upsert_annotations] DELETING existing annotations with ids: {ids_to_remove}')

    session: AsyncSession
    async with db_engine.session() as session:
        annotation: AnnotationModel | Annotation | None
        # TODO this seems too excessive compared to simply deleting them directly (but has to be done for FK constraint)
        #      session.execute(delete(Annotation).where(Annotation.annotation_id == annotation_id))
        annotations_to_delete = [
            (await session.scalars(select(Annotation).filter_by(annotation_id=annotation_id))).first()
            for annotation_id in ids_to_remove
        ]
        for annotation in annotations_to_delete:
            await session.delete(annotation)

        new_annotations = []
        for annotation in annotations:
            if str(annotation.annotation_id) in ids_to_create:
                new_annotations.append(Annotation(**annotation.model_dump()))

        session.add_all(new_annotations)
        await session.flush()

        for annotation in annotations:
            if annotation.annotation_id in ids_to_update:
                stmt = select(Annotation).filter_by(annotation_id=annotation.annotation_id)
                annotation_db = (await session.scalars(stmt)).one_or_none()
                if annotation_db is None:
                    raise RuntimeError('During processing, one of the annotations disappeared.'
                                       f'This should never happen! ID for reference: {annotation.annotation_id}')
                annotation_db.key = annotation.key
                annotation_db.repeat = annotation.repeat
                annotation_db.parent = annotation.parent
                annotation_db.value_int = annotation.value_int
                annotation_db.value_bool = annotation.value_bool
                annotation_db.value_str = annotation.value_str
                annotation_db.value_float = annotation.value_float
                await session.flush()
        await session.commit()

    if assignment_id is not None:
        annotation_scheme = await read_annotation_scheme_for_assignment(assignment_id=assignment_id,
                                                                        db_engine=db_engine)
        if annotation_scheme is not None:
            status = validate_annotated_assignment(annotation_scheme=annotation_scheme, annotations=annotations)
            await update_assignment_status(assignment_id=assignment_id, status=status, db_engine=db_engine, use_commit=True)
            return status

    return None


class ItemWithCount(BaseModel):
    item_id: uuid.UUID | str
    num_total: int
    num_open: int
    num_partial: int
    num_full: int


@ensure_session_async
async def read_item_ids_with_assignment_count_for_project(session: DBSession,
                                                          project_id: str | uuid.UUID) -> list[ItemWithCount]:
    stmt = text("""
    SELECT assignment.item_id,
           COUNT(DISTINCT assignment.assignment_id) AS num_total,
           SUM(CASE WHEN assignment.status = 'OPEN' THEN 1 ELSE 0 END) AS num_open,
           SUM(CASE WHEN assignment.status = 'PARTIAL' THEN 1 ELSE 0 END) AS num_partial,
           SUM(CASE WHEN assignment.status = 'FULL' THEN 1 ELSE 0 END) AS num_full
    FROM assignment
    JOIN annotation_scheme ON annotation_scheme.annotation_scheme_id = assignment.annotation_scheme_id
    WHERE annotation_scheme.project_id = :project_id
    GROUP BY assignment.item_id;
    """)
    result = (await session.execute(stmt, {'project_id': project_id})).mappings().all()
    return [ItemWithCount(**res) for res in result]  # type: ignore[misc]


class AssignmentCounts(BaseModel):
    num_total: int
    num_open: int
    num_partial: int
    num_full: int


@ensure_session_async
async def read_assignment_counts_for_scope(session: DBSession,
                                           assignment_scope_id: str | uuid.UUID) -> AssignmentCounts:
    stmt = text("""
    SELECT COUNT(DISTINCT assignment.assignment_id)                       AS num_total,
           SUM(CASE WHEN assignment.status = 'OPEN' THEN 1 ELSE 0 END)    AS num_open,
           SUM(CASE WHEN assignment.status = 'PARTIAL' THEN 1 ELSE 0 END) AS num_partial,
           SUM(CASE WHEN assignment.status = 'FULL' THEN 1 ELSE 0 END)    AS num_full
    FROM assignment
    WHERE assignment_scope_id = :assignment_scope_id
    GROUP BY assignment_scope_id;
    """)
    result = (await session.execute(stmt, {'assignment_scope_id': assignment_scope_id})).mappings().one_or_none()

    if result is None:
        return AssignmentCounts(num_full=0, num_open=0, num_total=0, num_partial=0)

    return AssignmentCounts(**result)  # type: ignore[misc]


@ensure_session_async
async def store_assignments(session: DBSession,
                            assignments: list[AssignmentModel]) -> None:
    assignments_orm = [Assignment(**assignment.model_dump()) for assignment in assignments]
    session.add_all(assignments_orm)
    await session.flush_or_commit()


@ensure_session_async
async def read_resolved_bot_annotation_meta(session: DBSession,
                                            bot_annotation_metadata_id: str) -> BotAnnotationResolution:
    stmt = (select(BotAnnotationMetaData)
            .where(BotAnnotationMetaData.bot_annotation_metadata_id == bot_annotation_metadata_id))
    rslt = (await session.execute(stmt)).scalars().one_or_none()
    if rslt is None:
        raise NotFoundError(f'No bot_annotation with id={bot_annotation_metadata_id}')
    return BotAnnotationResolution.model_validate(rslt.__dict__)


@ensure_session_async
async def read_resolved_bot_annotations_for_meta(session: DBSession,
                                                 bot_meta: BotAnnotationResolution,
                                                 include_empty: bool = True,
                                                 include_new: bool = False,
                                                 update_existing: bool = False) -> ResolutionProposal:
    ret: ResolutionProposal = await get_resolved_item_annotations(strategy=bot_meta.meta.algorithm,
                                                                  assignment_scope_id=bot_meta.assignment_scope_id,
                                                                  ignore_hierarchy=bot_meta.meta.ignore_hierarchy,
                                                                  ignore_repeat=bot_meta.meta.ignore_repeat,
                                                                  include_empty=include_empty,
                                                                  include_new=include_new,
                                                                  update_existing=update_existing,
                                                                  bot_meta=bot_meta,
                                                                  session=session)
    return ret


@ensure_session_async
async def read_resolved_bot_annotations(session: DBSession,
                                        existing_resolution: str,
                                        include_empty: bool = True,
                                        include_new: bool = False,
                                        update_existing: bool = False) -> ResolutionProposal:
    bot_meta = await read_resolved_bot_annotation_meta(bot_annotation_metadata_id=existing_resolution,
                                                       session=session)
    return await read_resolved_bot_annotations_for_meta(session=session,
                                                        bot_meta=bot_meta,
                                                        include_new=include_new,
                                                        include_empty=include_empty,
                                                        update_existing=update_existing)


@ensure_session_async
async def store_resolved_bot_annotations(session: DBSession,
                                         project_id: str,
                                         name: str,
                                         assignment_scope_id: str | uuid.UUID,
                                         annotation_scheme_id: str | uuid.UUID,
                                         algorithm: ResolutionMethod,
                                         ignore_hierarchy: bool,
                                         ignore_repeat: bool,
                                         matrix: ResolutionMatrix) -> str:
    snapshot = dehydrate_user_annotations(matrix)
    resolutions = dehydrate_resolutions(matrix)
    meta = BotMetaResolve(algorithm=algorithm,
                          ignore_hierarchy=ignore_hierarchy,
                          ignore_repeat=ignore_repeat,
                          snapshot=snapshot,
                          resolutions=resolutions)
    meta_uuid = uuid.uuid4()
    metadata = BotAnnotationMetaData(bot_annotation_metadata_id=meta_uuid,
                                     project_id=project_id,
                                     name=name,
                                     kind=BotKind.RESOLVE,
                                     annotation_scheme_id=annotation_scheme_id,
                                     assignment_scope_id=assignment_scope_id,
                                     meta=meta.model_dump())
    session.add(metadata)
    await session.flush()

    # We are assuming, that the parent linkages are already done!
    for row in matrix.values():
        for cell in row.values():
            if cell.resolution and has_values(cell.resolution):
                ba_dump = cell.resolution.model_dump()
                ba_dump['bot_annotation_metadata_id'] = meta_uuid
                ba_dump['bot_annotation_id'] = uuid.UUID(ba_dump['bot_annotation_id'])
                session.add(BotAnnotation(**ba_dump))
            # FIXME: When a parent is empty, it will be excluded, leading to a missing foreign key error!
            #        However, just adding all BA's will add lots of junk to the database...

    await session.flush_or_commit()

    return str(meta_uuid)


def has_changed(orm: BotAnnotation, resolution: BotAnnotationModel) -> bool:
    return (orm.repeat != resolution.repeat
            or str(orm.parent or 'None') != str(resolution.parent or 'None')
            or orm.order != resolution.order
            or orm.value_bool != resolution.value_bool
            or orm.value_str != resolution.value_str
            or orm.value_int != resolution.value_int
            or orm.value_float != resolution.value_float
            or orm.multi_int != resolution.multi_int)


@ensure_session_async
async def update_resolved_bot_annotations(session: DBSession,
                                          bot_annotation_metadata_id: str,
                                          name: str,
                                          matrix: ResolutionMatrix) -> None:
    bot_meta: BotAnnotationMetaData | None = (await session.execute(
        select(BotAnnotationMetaData)
        .where(BotAnnotationMetaData.bot_annotation_metadata_id == bot_annotation_metadata_id))) \
        .scalars().one_or_none()

    if bot_meta is None:
        raise MissingIdError(f'No `BotAnnotationMetaData` object for {bot_annotation_metadata_id}')

    resolutions = dehydrate_resolutions(matrix)
    # Update the bot_meta
    bot_meta.name = name
    bot_meta.meta.snapshot = dehydrate_user_annotations(matrix)
    bot_meta.meta.resolutions = resolutions
    await session.flush()

    # Get ids of existing annotations in for this resolution
    existing_ids_uuid: list[uuid.UUID] = list(
        (await session.execute(
            select(BotAnnotation.bot_annotation_id)
            .where(BotAnnotation.bot_annotation_metadata_id == bot_annotation_metadata_id)))
        .scalars().all())

    # Figure out which ones we have to delete, update, and create
    existing_ids = set([str(ba) for ba in existing_ids_uuid])
    submitted_ids = set([str(res.ba_id) for res in resolutions])

    ids_to_remove = existing_ids - submitted_ids
    ids_to_update = existing_ids - ids_to_remove
    ids_to_create = submitted_ids - ids_to_update

    logger.debug(f'[upsert_bot_annotations] CREATING ({len(ids_to_create):,}) '
                 f'new annotations with ids: {ids_to_create}')
    logger.debug(f'[upsert_bot_annotations] UPDATING ({len(ids_to_update):,}) '
                 f'existing annotations with ids: {ids_to_update}')
    logger.debug(f'[upsert_bot_annotations] DELETING ({len(ids_to_remove):,}) '
                 f'existing annotations with ids: {ids_to_remove}')

    # Delete old bot_annotations
    if len(ids_to_remove) > 0:
        stmt = delete(BotAnnotation).where(BotAnnotation.bot_annotation_id.in_(list(ids_to_remove)))
        await session.execute(stmt)

    # Create new or update existing bot_annotations
    new_bot_annotations = []
    for row in matrix.values():
        for cell in row.values():
            if cell.resolution:
                if str(cell.resolution.bot_annotation_id) in ids_to_update:
                    if has_values(cell.resolution):
                        ba_orm: BotAnnotation = (await session.execute(
                            select(BotAnnotation)
                            .where(
                                BotAnnotation.bot_annotation_id == cell.resolution.bot_annotation_id))).scalars().one()
                        if has_changed(ba_orm, cell.resolution):
                            ba_orm.repeat = cell.resolution.repeat
                            ba_orm.parent = cell.resolution.parent
                            ba_orm.value_bool = cell.resolution.value_bool
                            ba_orm.value_str = cell.resolution.value_str
                            ba_orm.value_int = cell.resolution.value_int
                            ba_orm.value_float = cell.resolution.value_float
                            ba_orm.multi_int = cell.resolution.multi_int
                            ba_orm.order = cell.resolution.order
                            await session.flush()
                    else:
                        await session.execute(
                            delete(BotAnnotation)
                            .where(BotAnnotation.bot_annotation_id == cell.resolution.bot_annotation_id)
                        )
                else:
                    if has_values(cell.resolution):
                        new_orm = BotAnnotation(**{
                            **cell.resolution.model_dump(),
                            'bot_annotation_metadata_id': uuid.UUID(bot_annotation_metadata_id)
                        })
                        new_orm.item_id = uuid.UUID(new_orm.item_id)
                        new_orm.bot_annotation_id = uuid.UUID(new_orm.bot_annotation_id)
                        if new_orm.parent:
                            new_orm.parent = uuid.UUID(new_orm.parent)
                        new_bot_annotations.append(new_orm)
    if len(new_bot_annotations) > 0:
        logger.debug(f'Creating ({len(new_bot_annotations):,} new BotAnnotations.')
        session.add_all(new_bot_annotations)
        await session.flush()

    await session.flush_or_commit()
