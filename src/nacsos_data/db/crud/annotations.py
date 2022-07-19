import uuid
from uuid import UUID
import logging

from sqlalchemy import select, delete, asc, desc, func, distinct
from sqlalchemy.sql import text
from pydantic import BaseModel

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import \
    Annotation, \
    AnnotationTask, \
    Assignment, \
    AssignmentScope, \
    Item
from nacsos_data.models.annotations import \
    AnnotationModel, \
    AnnotationTaskModel, \
    AssignmentModel, \
    AssignmentScopeModel, \
    AssignmentStatus
from nacsos_data.models.items import \
    ItemModel
from nacsos_data.models.projects import \
    ProjectModel, \
    ProjectPermissionsModel
from nacsos_data.util.annotations.validation import validate_annotated_assignment, merge_task_and_annotations
from . import update_orm, upsert_orm

logger = logging.getLogger('nacsos_data.crud.annotations')


class UserProjectAssignmentScope(BaseModel):
    scope: AssignmentScopeModel
    task_name: str
    task_description: str
    num_assignments: int
    num_open: int
    num_partial: int
    num_completed: int


async def read_assignment_scopes_for_project_for_user(project_id: str | UUID,
                                                      user_id: str | UUID,
                                                      engine: DatabaseEngineAsync) -> list[UserProjectAssignmentScope]:
    async with engine.session() as session:
        stmt = text("""
        SELECT scope.*,
               task.name AS task_name,
               task.description AS task_description,
               COUNT(DISTINCT assi.assignment_id) AS num_assignments,
               SUM(CASE WHEN assi.status = 'OPEN' THEN 1 ELSE 0 END) AS num_open,
               SUM(CASE WHEN assi.status = 'PARTIAL' THEN 1 ELSE 0 END) AS num_partial,
               SUM(CASE WHEN assi.status = 'FULL' THEN 1 ELSE 0 END) AS num_completed
        FROM assignment_scope scope
                 FULL OUTER JOIN annotation_task task ON scope.task_id = task.annotation_task_id
                 LEFT OUTER JOIN assignment assi ON scope.assignment_scope_id = assi.assignment_scope_id
        WHERE assi.user_id = :user_id AND 
              task.project_id = :project_id
        GROUP BY scope.assignment_scope_id, task_name, task_description;
        """)
        result = await session.execute(stmt, {'user_id': user_id, 'project_id': project_id})
        result_list = result.mappings().all()
        return [
            UserProjectAssignmentScope(
                scope=AssignmentScopeModel(
                    assignment_scope_id=res['assignment_scope_id'],
                    task_id=res['task_id'],
                    name=res['name'],
                    description=res['description']
                ),
                task_name=res['task_name'],
                task_description=res['task_description'],
                num_assignments=res['num_assignments'],
                num_open=res['num_open'],
                num_partial=res['num_partial'],
                num_completed=res['num_completed'],
            )
            for res in result_list
        ]


async def read_assignment_scopes_for_project(project_id: str | UUID,
                                             engine: DatabaseEngineAsync) -> list[AssignmentScopeModel]:
    async with engine.session() as session:
        stmt = select(AssignmentScope) \
            .join(AnnotationTask, AnnotationTask.annotation_task_id == AssignmentScope.task_id) \
            .where(AnnotationTask.project_id == project_id) \
            .order_by(desc(AssignmentScope.time_created))
        result = (await session.execute(stmt)).scalars().all()
        return [AssignmentScopeModel(**res.__dict__) for res in result]


async def read_assignments_for_scope_for_user(assignment_scope_id: str | UUID, user_id: str | UUID,
                                              engine: DatabaseEngineAsync) -> list[AssignmentModel]:
    async with engine.session() as session:
        stmt = select(Assignment) \
            .filter_by(assignment_scope_id=assignment_scope_id, user_id=user_id) \
            .order_by(asc(Assignment.order))
        result = (await session.execute(stmt)).scalars().all()
        return [AssignmentModel(**res.__dict__) for res in result]


async def read_assignments_for_scope(assignment_scope_id: str | UUID,
                                     engine: DatabaseEngineAsync) -> list[AssignmentModel]:
    async with engine.session() as session:
        stmt = select(Assignment) \
            .filter_by(assignment_scope_id=assignment_scope_id) \
            .order_by(asc(Assignment.order))
        result = (await session.execute(stmt)).scalars().all()
        return [AssignmentModel(**res.__dict__) for res in result]


async def read_next_assignment_for_scope_for_user(current_assignment_id: str | UUID,
                                                  assignment_scope_id: str | UUID,
                                                  user_id: str | UUID,
                                                  engine: DatabaseEngineAsync) -> AssignmentModel:
    async with engine.session() as session:
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
        result = await session.execute(stmt, {'user_id': user_id,
                                              'assignment_scope_id': assignment_scope_id,
                                              'assignment_id': current_assignment_id})
        result = result.mappings().one_or_none()
        return AssignmentModel(**result)


async def read_next_open_assignment_for_scope_for_user(assignment_scope_id: str | UUID,
                                                       user_id: str | UUID,
                                                       engine: DatabaseEngineAsync) -> AssignmentModel:
    async with engine.session() as session:
        stmt = select(Assignment) \
            .where(Assignment.user_id == user_id,
                   Assignment.assignment_scope_id == assignment_scope_id,
                   Assignment.status == 'OPEN') \
            .order_by(asc(Assignment.order)) \
            .limit(1)
        result = (await session.execute(stmt)).scalars().one_or_none()
        return AssignmentModel(**result.__dict__)


async def read_assignment(assignment_id: str | UUID,
                          engine: DatabaseEngineAsync) -> AssignmentModel:
    async with engine.session() as session:
        stmt = select(Assignment).filter_by(assignment_id=assignment_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        return AssignmentModel(**result.__dict__)


async def read_annotations_for_scope_for_user(assignment_scope_id: str | UUID, user_id: str | UUID,
                                              engine: DatabaseEngineAsync) -> list[AnnotationModel]:
    async with engine.session() as session:
        stmt = select(Annotation) \
            .join(Assignment, Assignment.assignment_id == Annotation.assignment_id) \
            .where(Assignment.assignment_scope_id == assignment_scope_id,
                   Assignment.user_id == user_id)
        result = (await session.execute(stmt)).scalars().all()
        return [AnnotationModel(**res.__dict__) for res in result]


async def read_annotations_for_assignment(assignment_id: str | UUID, engine: DatabaseEngineAsync) \
        -> list[AnnotationModel]:
    async with engine.session() as session:
        stmt = select(Annotation).filter_by(assignment_id=assignment_id)
        result = (await session.execute(stmt)).scalars().all()
        return [AnnotationModel(**res.__dict__) for res in result]


async def read_assignment_scope(assignment_scope_id: str | UUID, engine: DatabaseEngineAsync) \
        -> AssignmentScopeModel:
    async with engine.session() as session:
        stmt = select(AssignmentScope).filter_by(assignment_scope_id=assignment_scope_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AssignmentScopeModel(**result.__dict__)


async def read_annotation_task(annotation_task_id: str | UUID, engine: DatabaseEngineAsync) \
        -> AnnotationTaskModel:
    async with engine.session() as session:
        stmt = select(AnnotationTask).filter_by(annotation_task_id=annotation_task_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AnnotationTaskModel(**result.__dict__)


async def read_annotation_task_for_assignment(assignment_id: str | UUID, engine: DatabaseEngineAsync) \
        -> AnnotationTaskModel:
    async with engine.session() as session:
        stmt = select(AnnotationTask) \
            .join(Assignment, AnnotationTask.annotation_task_id == Assignment.task_id) \
            .where(Assignment.assignment_id == assignment_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AnnotationTaskModel(**result.__dict__)


async def read_annotation_task_for_scope(assignment_scope_id: str | UUID, engine: DatabaseEngineAsync) \
        -> AnnotationTaskModel:
    async with engine.session() as session:
        stmt = select(AnnotationTask) \
            .join(AssignmentScope, AssignmentScope.task_id == AnnotationTask.annotation_task_id) \
            .where(AssignmentScope.assignment_scope_id == assignment_scope_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AnnotationTaskModel(**result.__dict__)


async def read_annotation_tasks_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) \
        -> list[AnnotationTaskModel]:
    async with engine.session() as session:
        stmt = select(AnnotationTask).filter_by(project_id=project_id)
        result = (await session.execute(stmt)).scalars().all()
        return [AnnotationTaskModel(**res.__dict__) for res in result]


async def read_task_with_annotations(assignment_id: str | UUID,
                                     engine: DatabaseEngineAsync) -> AnnotationTaskModel:
    annotation_task = await read_annotation_task_for_assignment(assignment_id=assignment_id, engine=engine)
    annotations = await read_annotations_for_assignment(assignment_id=assignment_id, engine=engine)

    _, annotated_annotation_task = merge_task_and_annotations(annotation_task, annotations)
    return annotated_annotation_task


async def update_assignment_status(assignment_id: str | UUID, status: AssignmentStatus, engine: DatabaseEngineAsync):
    async with engine.session() as session:
        stmt = select(Assignment).where(Assignment.assignment_id == assignment_id)
        assignment: Assignment = (await session.scalars(stmt)).one()
        assignment.status = status
        await session.commit()


async def upsert_assignment_scope(assignment_scope: AssignmentScopeModel,
                                  engine: DatabaseEngineAsync) -> str | UUID | None:
    key = await upsert_orm(upsert_model=assignment_scope,
                           Schema=AssignmentScope,
                           primary_key=AssignmentScope.assignment_scope_id.name,
                           engine=engine)
    return key


async def delete_assignment_scope(assignment_scope_id: str | UUID,
                                  engine: DatabaseEngineAsync) -> None:
    async with engine.session() as session:
        stmt = select(AssignmentScope).filter_by(assignment_scope_id=assignment_scope_id)
        assignment_scope = (await session.scalars(stmt)).one_or_none()
        if assignment_scope is not None:
            await session.delete(assignment_scope)
            await session.commit()
        else:
            raise ValueError(f'Assignment scope with id="{assignment_scope_id}" does not seem to exist.')


async def upsert_annotations(annotations: list[AnnotationModel],
                             assignment_id: str | UUID | None,
                             engine: DatabaseEngineAsync) -> AssignmentStatus | None:
    if assignment_id is not None:
        existing_annotations = await read_annotations_for_assignment(assignment_id=assignment_id, engine=engine)
        existing_ids = set([str(annotation.annotation_id) for annotation in existing_annotations])
        submitted_ids = set([annotation.annotation_id for annotation in annotations])

        ids_to_remove = existing_ids - submitted_ids
        ids_to_update = existing_ids - ids_to_remove
        ids_to_create = submitted_ids - ids_to_update
    else:
        ids_to_remove = set()
        ids_to_update = set()
        ids_to_create = set([annotation.annotation_id for annotation in annotations])

    logger.debug(f'[upsert_annotations] CREATING new annotations with ids: {ids_to_create}')
    logger.debug(f'[upsert_annotations] UPDATING existing annotations with ids: {ids_to_update}')
    logger.debug(f'[upsert_annotations] DELETING existing annotations with ids: {ids_to_remove}')

    async with engine.session() as session:
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
            if annotation.annotation_id in ids_to_create:
                new_annotations.append(Annotation(**annotation.dict()))
        session.add_all(new_annotations)
        await session.commit()

        for annotation in annotations:
            if annotation.annotation_id in ids_to_update:
                stmt = select(Annotation).where(Annotation.annotation_id == annotation.annotation_id)
                annotation_db: Annotation = (await session.scalars(stmt)).one()
                annotation_db.key = annotation.key
                annotation_db.repeat = annotation.repeat
                annotation_db.parent = annotation.parent
                annotation_db.value_int = annotation.value_int
                annotation_db.value_bool = annotation.value_bool
                annotation_db.value_str = annotation.value_str
                annotation_db.value_float = annotation.value_float
                await session.commit()

    if assignment_id is not None:
        annotation_task = await read_annotation_task_for_assignment(assignment_id=assignment_id, engine=engine)
        status = validate_annotated_assignment(annotation_task=annotation_task, annotations=annotations)
        await update_assignment_status(assignment_id=assignment_id, status=status, engine=engine)
        return status


class ItemWithCount(BaseModel):
    item_id: UUID | str
    num_total: int
    num_open: int
    num_partial: int
    num_full: int


async def read_item_ids_with_assignment_count_for_project(project_id: str | UUID,
                                                          engine: DatabaseEngineAsync) -> list[ItemWithCount]:
    async with engine.session() as session:
        stmt = text("""
        SELECT assignment.item_id,
               COUNT(DISTINCT assignment.assignment_id) AS num_total,
               SUM(CASE WHEN assignment.status = 'OPEN' THEN 1 ELSE 0 END) AS num_open,
               SUM(CASE WHEN assignment.status = 'PARTIAL' THEN 1 ELSE 0 END) AS num_partial,
               SUM(CASE WHEN assignment.status = 'FULL' THEN 1 ELSE 0 END) AS num_full
        FROM assignment 
        JOIN annotation_task ON annotation_task.annotation_task_id = assignment.task_id 
        WHERE annotation_task.project_id = :project_id
        GROUP BY assignment.item_id;
        """)
        result = (await session.execute(stmt, {'project_id': project_id})).mappings().all()
        return [ItemWithCount(**res) for res in result]


class AssignmentCounts(BaseModel):
    num_total: int
    num_open: int
    num_partial: int
    num_full: int


async def read_assignment_counts_for_scope(assignment_scope_id: str | UUID,
                                           engine: DatabaseEngineAsync) -> AssignmentCounts:
    async with engine.session() as session:
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
                            engine: DatabaseEngineAsync) -> None:
    async with engine.session() as session:
        assignments_orm = [Assignment(**assignment.dict()) for assignment in assignments]
        session.add_all(assignments_orm)
        await session.commit()
