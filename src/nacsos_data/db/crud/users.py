from sqlalchemy import select, asc
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import User, ProjectPermissions
from nacsos_data.models.users import UserInDBModel, UserModel


async def read_user_by_id(user_id: str, engine: DatabaseEngineAsync) -> UserInDBModel | None:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(User).filter_by(user_id=user_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return UserInDBModel(**result.__dict__)
    return None


async def read_users_by_ids(user_ids: list[str], engine: DatabaseEngineAsync) -> list[UserInDBModel] | None:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(User).filter(User.user_id.in_(user_ids))
        result = (await session.execute(stmt)).scalars().all()
        if result is not None:
            return [UserInDBModel(**res.__dict__) for res in result]
    return None


async def read_user_by_name(username: str, engine: DatabaseEngineAsync) -> UserInDBModel | None:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(User).filter_by(username=username)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return UserInDBModel(**result.__dict__)
    return None


async def read_all_users(engine: DatabaseEngineAsync) -> list[UserInDBModel]:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(User)
        result = (await session.execute(stmt)).scalars().all()
        return [UserInDBModel(**res.__dict__) for res in result]


async def read_project_users(project_id: str | UUID, engine: DatabaseEngineAsync) -> list[UserInDBModel] | None:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(User) \
            .join(ProjectPermissions, ProjectPermissions.user_id == User.user_id) \
            .where(ProjectPermissions.project_id == project_id) \
            .order_by(asc(User.username))
        result = (await session.execute(stmt)).scalars().all()
        if result is not None:
            return [UserInDBModel(**res.__dict__) for res in result]
    return None


def update_user(user: UserModel) -> str | UUID:
    # TODO implement update function
    # first check, that ID is set
    pass


def delete_user(uid: str) -> bool:
    # TODO delete user with user_id uid
    pass
