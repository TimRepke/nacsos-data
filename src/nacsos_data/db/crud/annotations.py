import uuid
import logging
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy import select, delete, asc, desc
from sqlalchemy.sql import text

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import \
    Annotation, \
    AnnotationScheme, \
    Assignment, \
    AssignmentScope, \
    BotAnnotationMetaData, \
    BotAnnotation
from nacsos_data.models.annotations import \
    AnnotationModel, \
    AnnotationSchemeModel, \
    AssignmentModel, \
    AssignmentScopeModel, \
    AssignmentStatus
from nacsos_data.models.bot_annotations import \
    AnnotationFilters, \
    BotMetaResolve, \
    ResolutionMethod, \
    AnnotationCollectionDB, \
    BotKind, \
    BotAnnotationModel
from nacsos_data.util.annotations.validation import validate_annotated_assignment, merge_scheme_and_annotations

from . import upsert_orm, MissingIdError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401

logger = logging.getLogger('nacsos_data.crud.annotations')


class UserProjectAssignmentScope(BaseModel):
    scope: AssignmentScopeModel
    scheme_name: str
    scheme_description: str
    num_assignments: int
    num_open: int
    num_partial: int
    num_completed: int


async def read_assignment_scopes_for_project_for_user(project_id: str | uuid.UUID,
                                                      user_id: str | uuid.UUID,
                                                      db_engine: DatabaseEngineAsync) \
        -> list[UserProjectAssignmentScope]:
    async with db_engine.session() as session:  # type: AsyncSession
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
                    description=res['description']
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


async def read_assignment_scopes_for_project(project_id: str | uuid.UUID,
                                             db_engine: DatabaseEngineAsync) -> list[AssignmentScopeModel]:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = (select(AssignmentScope)
                .join(AnnotationScheme,
                      AnnotationScheme.annotation_scheme_id == AssignmentScope.annotation_scheme_id)
                .where(AnnotationScheme.project_id == project_id)
                .order_by(desc(AssignmentScope.time_created)))
        result = (await session.execute(stmt)).scalars().all()
        return [AssignmentScopeModel(**res.__dict__) for res in result]


async def read_assignments_for_scope_for_user(assignment_scope_id: str | uuid.UUID,
                                              user_id: str | uuid.UUID,
                                              db_engine: DatabaseEngineAsync,
                                              limit: int | None = None) -> list[AssignmentModel]:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(Assignment) \
            .filter_by(assignment_scope_id=assignment_scope_id, user_id=user_id) \
            .order_by(asc(Assignment.order))
        if limit is not None:
            stmt = stmt.limit(limit)

        result = (await session.execute(stmt)).scalars().all()
        return [AssignmentModel(**res.__dict__) for res in result]


async def read_assignments_for_scope(assignment_scope_id: str | uuid.UUID,
                                     db_engine: DatabaseEngineAsync) -> list[AssignmentModel]:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(Assignment) \
            .filter_by(assignment_scope_id=assignment_scope_id) \
            .order_by(asc(Assignment.order))
        result = (await session.execute(stmt)).scalars().all()
        return [AssignmentModel(**res.__dict__) for res in result]


async def read_next_assignment_for_scope_for_user(current_assignment_id: str | uuid.UUID,
                                                  assignment_scope_id: str | uuid.UUID,
                                                  user_id: str | uuid.UUID,
                                                  db_engine: DatabaseEngineAsync) -> AssignmentModel | None:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = text("""
        WITH tmp as (SELECT assignment_id,
                        LEAD(assignment_id, 1) over (order by "order") as next_assignment_id
                 FROM assignment
                 WHERE user_id = :user_id AND assignment_scope_id=:assignment_scope_id)
        SELECT assignment.*
        FROM assignment
            JOIN tmp  ON  tmp.next_assignment_id=assignment.assignment_id
        WHERE tmp.assignment_id=:assignment_id;
        """)
        result = (await session.execute(stmt, {'user_id': user_id,
                                               'assignment_scope_id': assignment_scope_id,
                                               'assignment_id': current_assignment_id})).mappings().one_or_none()
        if result is not None:
            return AssignmentModel(**result)

    # Try to fall back on next open assignment (apparently we reached the end of the list here).
    return await read_next_open_assignment_for_scope_for_user(assignment_scope_id=assignment_scope_id,
                                                              user_id=user_id,
                                                              db_engine=db_engine)


async def read_next_open_assignment_for_scope_for_user(assignment_scope_id: str | uuid.UUID,
                                                       user_id: str | uuid.UUID,
                                                       db_engine: DatabaseEngineAsync) -> AssignmentModel | None:
    async with db_engine.session() as session:  # type: AsyncSession
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


async def read_assignment(assignment_id: str | uuid.UUID,
                          db_engine: DatabaseEngineAsync) -> AssignmentModel | None:
    async with db_engine.session() as session:
        stmt = select(Assignment).filter_by(assignment_id=assignment_id)
        result = (await session.execute(stmt)).scalars().one_or_none()

        if result is None:
            return None

        return AssignmentModel(**result.__dict__)


async def read_annotations_for_scope_for_user(assignment_scope_id: str | uuid.UUID,
                                              user_id: str | uuid.UUID,
                                              db_engine: DatabaseEngineAsync) -> list[AnnotationModel]:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = (select(Annotation)
                .join(Assignment, Assignment.assignment_id == Annotation.assignment_id)
                .where(Assignment.assignment_scope_id == assignment_scope_id,
                       Assignment.user_id == user_id))
        result = (await session.execute(stmt)).scalars().all()
        return [AnnotationModel(**res.__dict__) for res in result]


async def read_annotations_for_assignment(assignment_id: str | uuid.UUID,
                                          db_engine: DatabaseEngineAsync) -> list[AnnotationModel]:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(Annotation).filter_by(assignment_id=assignment_id)
        result = (await session.execute(stmt)).scalars().all()
        return [AnnotationModel(**res.__dict__) for res in result]


async def read_assignment_scope(assignment_scope_id: str | uuid.UUID,
                                db_engine: DatabaseEngineAsync) -> AssignmentScopeModel | None:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(AssignmentScope).filter_by(assignment_scope_id=assignment_scope_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AssignmentScopeModel(**result.__dict__)
    return None


async def read_annotation_scheme(annotation_scheme_id: str | uuid.UUID,
                                 db_engine: DatabaseEngineAsync) -> AnnotationSchemeModel | None:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(AnnotationScheme).filter_by(annotation_scheme_id=annotation_scheme_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AnnotationSchemeModel(**result.__dict__)
    return None


async def read_annotation_scheme_for_assignment(assignment_id: str | uuid.UUID,
                                                db_engine: DatabaseEngineAsync) -> AnnotationSchemeModel | None:
    async with db_engine.session() as session:
        stmt = (select(AnnotationScheme)
                .join(Assignment,
                      AnnotationScheme.annotation_scheme_id == Assignment.annotation_scheme_id)
                .where(Assignment.assignment_id == assignment_id))
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AnnotationSchemeModel(**result.__dict__)
    return None


async def read_annotation_scheme_for_scope(assignment_scope_id: str | uuid.UUID,
                                           db_engine: DatabaseEngineAsync) -> AnnotationSchemeModel | None:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = (select(AnnotationScheme)
                .join(AssignmentScope,
                      AssignmentScope.annotation_scheme_id == AnnotationScheme.annotation_scheme_id)
                .where(AssignmentScope.assignment_scope_id == assignment_scope_id))
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AnnotationSchemeModel(**result.__dict__)
    return None


async def read_annotation_schemes_for_project(project_id: str | uuid.UUID,
                                              db_engine: DatabaseEngineAsync) -> list[AnnotationSchemeModel]:
    async with db_engine.session() as session:  # type: AsyncSession
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


async def update_assignment_status(assignment_id: str | uuid.UUID, status: AssignmentStatus,
                                   db_engine: DatabaseEngineAsync) -> None:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(Assignment).where(Assignment.assignment_id == assignment_id)
        assignment: Assignment = (await session.scalars(stmt)).one()
        assignment.status = status
        await session.commit()


async def upsert_annotation_scheme(annotation_scheme: AnnotationSchemeModel,
                                   db_engine: DatabaseEngineAsync) -> str | uuid.UUID | None:
    key = await upsert_orm(upsert_model=annotation_scheme,
                           Schema=AnnotationScheme,
                           primary_key=AnnotationScheme.annotation_scheme_id.name,
                           db_engine=db_engine)
    return key


async def delete_annotation_scheme(annotation_scheme_id: str | uuid.UUID,
                                   db_engine: DatabaseEngineAsync) -> None:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(AnnotationScheme).filter_by(annotation_scheme_id=annotation_scheme_id)
        annotation_scheme = (await session.scalars(stmt)).one_or_none()
        if annotation_scheme is not None:
            await session.delete(annotation_scheme)
            await session.commit()
        else:
            raise ValueError(f'AnnotationScheme with id="{annotation_scheme_id}" does not seem to exist.')


async def upsert_assignment_scope(assignment_scope: AssignmentScopeModel,
                                  db_engine: DatabaseEngineAsync) -> str | uuid.UUID | None:
    key = await upsert_orm(upsert_model=assignment_scope,
                           Schema=AssignmentScope,
                           primary_key=AssignmentScope.assignment_scope_id.name,
                           db_engine=db_engine)
    return key


async def delete_assignment_scope(assignment_scope_id: str | uuid.UUID,
                                  db_engine: DatabaseEngineAsync) -> None:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(AssignmentScope).filter_by(assignment_scope_id=assignment_scope_id)
        assignment_scope = (await session.scalars(stmt)).one_or_none()
        if assignment_scope is not None:
            await session.delete(assignment_scope)
            await session.commit()
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

    async with db_engine.session() as session:  # type: AsyncSession
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
                new_annotations.append(Annotation(**annotation.dict()))

        session.add_all(new_annotations)
        await session.commit()

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
                await session.commit()

    if assignment_id is not None:
        annotation_scheme = await read_annotation_scheme_for_assignment(assignment_id=assignment_id,
                                                                        db_engine=db_engine)
        if annotation_scheme is not None:
            status = validate_annotated_assignment(annotation_scheme=annotation_scheme, annotations=annotations)
            await update_assignment_status(assignment_id=assignment_id, status=status, db_engine=db_engine)
            return status

    return None


class ItemWithCount(BaseModel):
    item_id: uuid.UUID | str
    num_total: int
    num_open: int
    num_partial: int
    num_full: int


async def read_item_ids_with_assignment_count_for_project(project_id: str | uuid.UUID,
                                                          db_engine: DatabaseEngineAsync) -> list[ItemWithCount]:
    async with db_engine.session() as session:  # type: AsyncSession
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
        return [ItemWithCount(**res) for res in result]


class AssignmentCounts(BaseModel):
    num_total: int
    num_open: int
    num_partial: int
    num_full: int


async def read_assignment_counts_for_scope(assignment_scope_id: str | uuid.UUID,
                                           db_engine: DatabaseEngineAsync) -> AssignmentCounts:
    async with db_engine.session() as session:  # type: AsyncSession
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

        return AssignmentCounts(**result)


async def store_assignments(assignments: list[AssignmentModel],
                            db_engine: DatabaseEngineAsync) -> None:
    async with db_engine.session() as session:  # type: AsyncSession
        assignments_orm = [Assignment(**assignment.dict()) for assignment in assignments]
        session.add_all(assignments_orm)
        await session.commit()


async def store_resolved_bot_annotations(project_id: str, name: str,
                                         algorithm: ResolutionMethod, filters: AnnotationFilters,
                                         ignore_hierarchy: bool, ignore_repeat: bool,
                                         collection: AnnotationCollectionDB, bot_annotations: list[BotAnnotationModel],
                                         db_engine: DatabaseEngineAsync) -> str:
    async with db_engine.session() as session:  # type: AsyncSession
        meta = BotMetaResolve(algorithm=algorithm, filters=filters,
                              ignore_hierarchy=ignore_hierarchy, ignore_repeat=ignore_repeat,
                              collection=collection)
        meta_uuid = uuid.uuid4()
        # TODO: should we also store assignment_scope_id? might be more than one...
        metadata = BotAnnotationMetaData(bot_annotation_metadata_id=meta_uuid,
                                         name=name, kind=BotKind.RESOLVE, project_id=project_id,
                                         annotation_scheme_id=filters.scheme_id, meta=meta.dict())
        session.add(metadata)
        await session.commit()

        annotation_heap = {anno.bot_annotation_id: anno for anno in bot_annotations}
        added_ids = set()

        while len(annotation_heap) > 0:
            to_add = [
                anno
                for anno in annotation_heap.values()
                if anno.parent is None or anno.parent in added_ids
            ]
            added_ids.update([anno.bot_annotation_id for anno in to_add])

            for anno in to_add:
                session.add(BotAnnotation(**{**anno.dict(), 'bot_annotation_metadata_id': meta_uuid}))
                await session.commit()
                del annotation_heap[anno.bot_annotation_id]

        return str(meta_uuid)


async def update_resolved_bot_annotations(bot_annotation_metadata_id: str,
                                          name: str,
                                          bot_annotations: list[BotAnnotationModel],
                                          db_engine: DatabaseEngineAsync) -> None:
    async with db_engine.session() as session:  # type: AsyncSession
        metadata: BotAnnotationMetaData | None = (await session.execute(
            select(BotAnnotationMetaData)
            .where(BotAnnotationMetaData.bot_annotation_metadata_id == bot_annotation_metadata_id))) \
            .scalars().one_or_none()
        if metadata is None:
            raise MissingIdError(f'No `BotAnnotationMetaData` object for {bot_annotation_metadata_id}')

        # update the name
        metadata.name = name
        await session.commit()

        bot_annotations_orm: list[BotAnnotation] = list(
            (await session.execute(
                select(BotAnnotation)
                .where(BotAnnotation.bot_annotation_metadata_id == bot_annotation_metadata_id)))
            .scalars().all())

        existing_ids = set([str(ba.bot_annotation_id) for ba in bot_annotations_orm])
        submitted_ids = set([str(ba.bot_annotation_id) for ba in bot_annotations])

        ids_to_remove = existing_ids - submitted_ids
        ids_to_update = existing_ids - ids_to_remove
        ids_to_create = submitted_ids - ids_to_update

        logger.debug(f'[upsert_bot_annotations] CREATING new annotations with ids: {ids_to_create}')
        logger.debug(f'[upsert_bot_annotations] UPDATING existing annotations with ids: {ids_to_update}')
        logger.debug(f'[upsert_bot_annotations] DELETING existing annotations with ids: {ids_to_remove}')

        if len(ids_to_remove) > 0:
            stmt = delete(BotAnnotation).where(BotAnnotation.bot_annotation_id.in_(list(ids_to_remove)))
            await session.execute(stmt)

        bot_annotations_to_be_created = []
        for annotation in bot_annotations:
            if annotation.bot_annotation_id in ids_to_update:
                ba_orm: BotAnnotation = (await session.execute(
                    select(BotAnnotation)
                    .where(BotAnnotation.bot_annotation_id == annotation.bot_annotation_id))).scalars().one()
                ba_orm.repeat = annotation.repeat
                ba_orm.parent = annotation.parent
                ba_orm.value_bool = annotation.value_bool
                ba_orm.value_str = annotation.value_str
                ba_orm.value_int = annotation.value_int
                ba_orm.value_float = annotation.value_float
                ba_orm.multi_int = annotation.multi_int
                await session.commit()
            elif annotation.bot_annotation_id in ids_to_create:
                bot_annotations_to_be_created.append(BotAnnotation(**annotation.dict()))
        session.add_all(bot_annotations_to_be_created)
        await session.commit()
