from uuid import UUID
import logging

from sqlalchemy import select, asc, desc
from sqlalchemy.sql import text
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import \
    Annotation, \
    AnnotationScheme, \
    Assignment, \
    AssignmentScope
from nacsos_data.models.annotations import \
    AnnotationModel, \
    AnnotationSchemeModel, \
    AssignmentModel, \
    AssignmentScopeModel, \
    AssignmentStatus
from nacsos_data.util.annotations.validation import validate_annotated_assignment, merge_scheme_and_annotations
from . import upsert_orm

logger = logging.getLogger('nacsos_data.crud.annotations')


class UserProjectAssignmentScope(BaseModel):
    scope: AssignmentScopeModel
    scheme_name: str
    scheme_description: str
    num_assignments: int
    num_open: int
    num_partial: int
    num_completed: int


async def read_assignment_scopes_for_project_for_user(project_id: str | UUID,
                                                      user_id: str | UUID,
                                                      engine: DatabaseEngineAsync) -> list[UserProjectAssignmentScope]:
    async with engine.session() as session:  # type: AsyncSession
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
        GROUP BY scope.assignment_scope_id, scheme_name, scheme_description;
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


async def read_assignment_scopes_for_project(project_id: str | UUID,
                                             engine: DatabaseEngineAsync) -> list[AssignmentScopeModel]:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(AssignmentScope) \
            .join(AnnotationScheme, AnnotationScheme.annotation_scheme_id == AssignmentScope.scheme_id) \
            .where(AnnotationScheme.project_id == project_id) \
            .order_by(desc(AssignmentScope.time_created))
        result = (await session.execute(stmt)).scalars().all()
        return [AssignmentScopeModel(**res.__dict__) for res in result]


async def read_assignments_for_scope_for_user(assignment_scope_id: str | UUID, user_id: str | UUID,
                                              engine: DatabaseEngineAsync) -> list[AssignmentModel]:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(Assignment) \
            .filter_by(assignment_scope_id=assignment_scope_id, user_id=user_id) \
            .order_by(asc(Assignment.order))
        result = (await session.execute(stmt)).scalars().all()
        return [AssignmentModel(**res.__dict__) for res in result]


async def read_assignments_for_scope(assignment_scope_id: str | UUID,
                                     engine: DatabaseEngineAsync) -> list[AssignmentModel]:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(Assignment) \
            .filter_by(assignment_scope_id=assignment_scope_id) \
            .order_by(asc(Assignment.order))
        result = (await session.execute(stmt)).scalars().all()
        return [AssignmentModel(**res.__dict__) for res in result]


async def read_next_assignment_for_scope_for_user(current_assignment_id: str | UUID,
                                                  assignment_scope_id: str | UUID,
                                                  user_id: str | UUID,
                                                  engine: DatabaseEngineAsync) -> AssignmentModel | None:
    async with engine.session() as session:  # type: AsyncSession
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
        if result is not None:
            return AssignmentModel(**result.__dict__)
    return None


async def read_next_open_assignment_for_scope_for_user(assignment_scope_id: str | UUID,
                                                       user_id: str | UUID,
                                                       engine: DatabaseEngineAsync) -> AssignmentModel:
    async with engine.session() as session:  # type: AsyncSession
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
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(Assignment).filter_by(assignment_id=assignment_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        return AssignmentModel(**result.__dict__)


async def read_annotations_for_scope_for_user(assignment_scope_id: str | UUID, user_id: str | UUID,
                                              engine: DatabaseEngineAsync) -> list[AnnotationModel]:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(Annotation) \
            .join(Assignment, Assignment.assignment_id == Annotation.assignment_id) \
            .where(Assignment.assignment_scope_id == assignment_scope_id,
                   Assignment.user_id == user_id)
        result = (await session.execute(stmt)).scalars().all()
        return [AnnotationModel(**res.__dict__) for res in result]


async def read_annotations_for_assignment(assignment_id: str | UUID, engine: DatabaseEngineAsync) \
        -> list[AnnotationModel]:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(Annotation).filter_by(assignment_id=assignment_id)
        result = (await session.execute(stmt)).scalars().all()
        return [AnnotationModel(**res.__dict__) for res in result]


async def read_assignment_scope(assignment_scope_id: str | UUID, engine: DatabaseEngineAsync) \
        -> AssignmentScopeModel | None:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(AssignmentScope).filter_by(assignment_scope_id=assignment_scope_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AssignmentScopeModel(**result.__dict__)
    return None


async def read_annotation_scheme(annotation_scheme_id: str | UUID, engine: DatabaseEngineAsync) \
        -> AnnotationSchemeModel | None:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(AnnotationScheme).filter_by(annotation_scheme_id=annotation_scheme_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AnnotationSchemeModel(**result.__dict__)
    return None


async def read_annotation_scheme_for_assignment(assignment_id: str | UUID, engine: DatabaseEngineAsync) \
        -> AnnotationSchemeModel | None:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(AnnotationScheme) \
            .join(Assignment, AnnotationScheme.annotation_scheme_id == Assignment.annotation_scheme_id) \
            .where(Assignment.assignment_id == assignment_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AnnotationSchemeModel(**result.__dict__)
    return None


async def read_annotation_scheme_for_scope(assignment_scope_id: str | UUID, engine: DatabaseEngineAsync) \
        -> AnnotationSchemeModel | None:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(AnnotationScheme) \
            .join(AssignmentScope, AssignmentScope.annotation_scheme_id == AnnotationScheme.annotation_scheme_id) \
            .where(AssignmentScope.assignment_scope_id == assignment_scope_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return AnnotationSchemeModel(**result.__dict__)
    return None


async def read_annotation_schemes_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) \
        -> list[AnnotationSchemeModel]:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(AnnotationScheme).filter_by(project_id=project_id)
        result = (await session.execute(stmt)).scalars().all()
        return [AnnotationSchemeModel(**res.__dict__) for res in result]


async def read_scheme_with_annotations(assignment_id: str | UUID,
                                       engine: DatabaseEngineAsync) -> AnnotationSchemeModel | None:
    annotation_scheme = await read_annotation_scheme_for_assignment(assignment_id=assignment_id, engine=engine)
    annotations = await read_annotations_for_assignment(assignment_id=assignment_id, engine=engine)

    if annotation_scheme is not None and annotations is not None:
        annotated_annotation_scheme = merge_scheme_and_annotations(annotation_scheme, annotations)
        return annotated_annotation_scheme
    return None


async def update_assignment_status(assignment_id: str | UUID, status: AssignmentStatus,
                                   engine: DatabaseEngineAsync) -> None:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(Assignment).where(Assignment.assignment_id == assignment_id)
        assignment: Assignment = (await session.scalars(stmt)).one()
        assignment.status = status
        await session.commit()


async def upsert_annotation_scheme(annotation_scheme: AnnotationSchemeModel,
                                   engine: DatabaseEngineAsync) -> str | UUID | None:
    key = await upsert_orm(upsert_model=annotation_scheme,
                           Schema=AnnotationScheme,
                           primary_key=AnnotationScheme.annotation_scheme_id.name,
                           engine=engine)
    return key


async def delete_annotation_scheme(annotation_scheme_id: str | UUID,
                                   engine: DatabaseEngineAsync) -> None:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(AnnotationScheme).filter_by(annotation_scheme_id=annotation_scheme_id)
        annotation_scheme = (await session.scalars(stmt)).one_or_none()
        if annotation_scheme is not None:
            await session.delete(annotation_scheme)
            await session.commit()
        else:
            raise ValueError(f'AnnotationScheme with id="{annotation_scheme_id}" does not seem to exist.')


async def upsert_assignment_scope(assignment_scope: AssignmentScopeModel,
                                  engine: DatabaseEngineAsync) -> str | UUID | None:
    key = await upsert_orm(upsert_model=assignment_scope,
                           Schema=AssignmentScope,
                           primary_key=AssignmentScope.assignment_scope_id.name,
                           engine=engine)
    return key


async def delete_assignment_scope(assignment_scope_id: str | UUID,
                                  engine: DatabaseEngineAsync) -> None:
    async with engine.session() as session:  # type: AsyncSession
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
    if not all([annotation.annotation_id is not None for annotation in annotations]):
        raise ValueError('One or more annotations have no ID, this in undefined behaviour.')
    if assignment_id is not None:
        existing_annotations = await read_annotations_for_assignment(assignment_id=assignment_id, engine=engine)
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

    async with engine.session() as session:  # type: AsyncSession
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
        annotation_scheme = await read_annotation_scheme_for_assignment(assignment_id=assignment_id, engine=engine)
        if annotation_scheme is not None:
            status = validate_annotated_assignment(annotation_scheme=annotation_scheme, annotations=annotations)
            await update_assignment_status(assignment_id=assignment_id, status=status, engine=engine)
            return status

    return None


class ItemWithCount(BaseModel):
    item_id: UUID | str
    num_total: int
    num_open: int
    num_partial: int
    num_full: int


async def read_item_ids_with_assignment_count_for_project(project_id: str | UUID,
                                                          engine: DatabaseEngineAsync) -> list[ItemWithCount]:
    async with engine.session() as session:  # type: AsyncSession
        stmt = text("""
        SELECT assignment.item_id,
               COUNT(DISTINCT assignment.assignment_id) AS num_total,
               SUM(CASE WHEN assignment.status = 'OPEN' THEN 1 ELSE 0 END) AS num_open,
               SUM(CASE WHEN assignment.status = 'PARTIAL' THEN 1 ELSE 0 END) AS num_partial,
               SUM(CASE WHEN assignment.status = 'FULL' THEN 1 ELSE 0 END) AS num_full
        FROM assignment
        JOIN annotation_scheme ON annotation_scheme.annotation_scheme_id = assignment.scheme_id
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


async def read_assignment_counts_for_scope(assignment_scope_id: str | UUID,
                                           engine: DatabaseEngineAsync) -> AssignmentCounts:
    async with engine.session() as session:  # type: AsyncSession
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
    async with engine.session() as session:  # type: AsyncSession
        assignments_orm = [Assignment(**assignment.dict()) for assignment in assignments]
        session.add_all(assignments_orm)
        await session.commit()
