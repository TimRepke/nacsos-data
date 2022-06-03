from sqlalchemy import select, delete
from sqlalchemy.sql import text
from uuid import UUID
from pydantic import BaseModel
from typing import Type

from nacsos_data.db import DatabaseEngineAsync
from . import update_orm
from nacsos_data.db.schemas import Annotation, AssignmentScope, Assignment, AnnotationTask
from nacsos_data.models.annotations import AssignmentScopeModel, AssignmentModel, AnnotationModel, AnnotationTaskModel
from nacsos_data.models.projects import ProjectModel, ProjectPermissionsModel


class UserProjectAssignmentScope(BaseModel):
    scope: AssignmentScopeModel
    num_assignments: int
    num_completed: int


async def read_assignment_scopes_for_project_for_user(project_id: str | UUID, user_id: str | UUID,
                                                      engine: DatabaseEngineAsync) -> list[UserProjectAssignmentScope]:
    # FIXME: should this be somewhere in util/annotations/ ?
    # FIXME: num_completed gives a false impression, it's rather num_started_or_completed
    async with engine.session() as session:
        stmt = text("""
        SELECT scope.*,
               count(distinct assi.item_id)       AS num_assignments,
               count(distinct anno.assignment_id) AS num_completed
        FROM assignment_scope scope
                 FULL OUTER JOIN annotation_task task ON scope.task_id = task.annotation_task_id
                 LEFT OUTER JOIN assignment assi ON scope.assignment_scope_id = assi.assignment_scope_id
                 LEFT OUTER JOIN annotation anno ON assi.assignment_id = anno.assignment_id
        WHERE assi.user_id = :user_id
          AND task.project_id = :project_id
        GROUP BY scope.assignment_scope_id
        HAVING count(distinct assi.item_id) > 0;
        """)
        result = await session.execute(stmt, {'user_id': user_id, 'project_id': project_id})
        result_list = result.mappings().all()
        return [
            UserProjectAssignmentScope(
                scope=AssignmentScopeModel(
                    assignment_scope_id=res['assignment_scope_id'],
                    task=res['task_id'],
                    name=res['name'],
                    description=res['description']
                ),
                num_assignments=res['num_assignments'],
                num_completed=res['num_completed']
            )
            for res in result_list
        ]


async def read_assignments_for_scope_for_user(assignment_scope_id: str | UUID, user_id: str | UUID,
                                              engine: DatabaseEngineAsync) -> list[AssignmentModel]:
    async with engine.session() as session:
        stmt = select(Assignment).filter_by(assignment_scope_id=assignment_scope_id, user_id=user_id)
        result = (await session.execute(stmt)).scalars().all()
        return [AssignmentModel(assignment_id=res.assignment_id,
                                user=res.user_id,
                                item=res.item_id,
                                task=res.task_id) for res in result]


async def read_annotations_for_assignment(assignment_id: str | UUID, engine: DatabaseEngineAsync) \
        -> list[AnnotationModel]:
    async with engine.session() as session:
        stmt = select(Annotation).filter_by(assignment_id=assignment_id)
        result = (await session.execute(stmt)).scalars().all()
        return [AnnotationModel(**res.__dict__) for res in result]


async def read_annotation_task(annotation_task_id: str | UUID, engine: DatabaseEngineAsync) \
        -> AnnotationTaskModel:
    async with engine.session() as session:
        stmt = select(AnnotationTask).filter_by(annotation_task_id=annotation_task_id)
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
