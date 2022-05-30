from typing import Optional
from sqlalchemy import select

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import User
from nacsos_data.models.users import UserInDBModel, UserModel


async def read_user_by_id(uid: str, engine: DatabaseEngineAsync) -> Optional[UserInDBModel]:
    async with engine.session() as session:
        stmt = select(User).filter_by(user_id=uid)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return UserInDBModel(**result.__dict__)


async def read_user_by_name(username: str, engine: DatabaseEngineAsync) -> Optional[UserInDBModel]:
    async with engine.session() as session:
        stmt = select(User).filter_by(username=username)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return UserInDBModel(**result.__dict__)


async def read_all_users(engine: DatabaseEngineAsync) -> list[UserInDBModel]:
    async with engine.session() as session:
        stmt = select(User)
        result = await session.execute(stmt)
        return [UserInDBModel(**res.__dict__) for res in result.scalars().all()]


def read_project_users(pid: str) -> list[UserModel]:
    # TODO implement function that returns all users with access to a project (pid = project id)
    pass


def update_user(user: UserModel):
    # TODO implement update function
    # first check, that ID is set
    pass


def delete_user(uid: str):
    # TODO delete user with user_id uid
    pass
