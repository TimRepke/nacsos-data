from sqlalchemy import select, delete
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from . import update_orm
from nacsos_data.db.schemas import Project, ProjectPermissions
from nacsos_data.models.projects import ProjectModel, ProjectPermissionsModel


async def read_all_projects(engine: DatabaseEngineAsync) -> list[ProjectModel]:
    async with engine.session() as session:
        stmt = select(Project)
        result = await session.execute(stmt)
        result_list = result.scalars().all()
        return [ProjectModel(**res.__dict__) for res in result_list]


async def read_all_projects_for_user(user_id: str | UUID, engine: DatabaseEngineAsync) -> list[ProjectModel]:
    async with engine.session() as session:
        stmt = (select(Project)
                .join(ProjectPermissions,
                      Project.project_id == ProjectPermissions.project_id)
                .where(ProjectPermissions.user_id == user_id))
        result = await session.execute(stmt)
        result_list = result.scalars().all()
        return [ProjectModel(**res.__dict__) for res in result_list]


async def read_project_by_id(project_id: str | UUID, engine: DatabaseEngineAsync) -> ProjectModel | None:
    async with engine.session() as session:
        stmt = select(Project).filter_by(project_id=project_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return ProjectModel(**result.__dict__)
    return None


async def read_project_permissions_by_id(permissions_id: str | UUID,
                                         engine: DatabaseEngineAsync) -> ProjectPermissionsModel | None:
    async with engine.session() as session:
        stmt = select(ProjectPermissions).filter_by(project_permission_id=permissions_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return ProjectPermissionsModel(**result.__dict__)
    return None


async def read_project_permissions_for_project(project_id: str | UUID,
                                               engine: DatabaseEngineAsync) -> list[ProjectPermissionsModel]:
    async with engine.session() as session:
        stmt = select(ProjectPermissions).filter_by(project_id=project_id)
        result = await session.execute(stmt)
        result_list = result.scalars().all()
        return [ProjectPermissionsModel(**res.__dict__) for res in result_list]


async def read_project_permissions_for_user(user_id: str | UUID, project_id: str | UUID,
                                            engine: DatabaseEngineAsync) -> ProjectPermissionsModel | None:
    async with engine.session() as session:
        stmt = select(ProjectPermissions).filter_by(user_id=user_id, project_id=project_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return ProjectPermissionsModel(**result.__dict__)
    return None


async def create_project(project: ProjectModel, engine: DatabaseEngineAsync) -> ProjectModel:
    async with engine.session() as session:
        new_project = Project(**project.model_dump())
        session.add(new_project)
        await session.commit()

        # return the newly created project, so we get the UUID back
        return ProjectModel(**new_project.__dict__)


async def update_project(project: ProjectModel, engine: DatabaseEngineAsync) -> ProjectModel:
    return await update_orm(updated_model=project,  # type: ignore[no-any-return]
                            Schema=Project, Model=ProjectModel,
                            filter_by={'project_id': project.project_id},
                            skip_update=['project_id'], db_engine=engine, use_commit=True)


async def create_project_permissions(permissions: ProjectPermissionsModel,
                                     engine: DatabaseEngineAsync) -> ProjectPermissionsModel:
    async with engine.session() as session:
        new_permissions = ProjectPermissions(**permissions.model_dump())
        session.add(new_permissions)
        await session.commit()

        # return the newly created project, so we get the UUID back
        return ProjectPermissionsModel(**new_permissions.__dict__)


async def update_project_permissions(permissions: ProjectPermissionsModel,
                                     engine: DatabaseEngineAsync) -> ProjectPermissionsModel:
    return await update_orm(updated_model=permissions,  # type: ignore[no-any-return]
                            Schema=ProjectPermissions, Model=ProjectPermissionsModel,
                            filter_by={'project_permission_id': permissions.project_permission_id},
                            skip_update=['project_id', 'user_id', 'project_permission_id'],
                            db_engine=engine, use_commit=True)


async def delete_project_permissions(project_permission_id: UUID | str,
                                     engine: DatabaseEngineAsync) -> None:
    async with engine.session() as session:
        stmt = (delete(ProjectPermissions)
                .where(ProjectPermissions.project_permission_id == project_permission_id))
        await session.execute(stmt)
        await session.commit()
