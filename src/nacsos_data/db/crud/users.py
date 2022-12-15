from uuid import uuid4
from typing import TYPE_CHECKING

from sqlalchemy import select, asc
from passlib.context import CryptContext

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import User, ProjectPermissions
from nacsos_data.models.users import UserInDBModel, UserModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


async def authenticate_user_by_name(username: str, plain_password: str,
                                    engine: DatabaseEngineAsync) -> UserInDBModel | None:
    user = await read_user_by_name(username=username, engine=engine)
    if not user:
        return None
    if not verify_password(plain_password, user.password):
        return None
    return user


async def authenticate_user_by_id(user_id: str, plain_password: str,
                                  engine: DatabaseEngineAsync) -> UserInDBModel | None:
    user = await read_user_by_id(user_id=user_id, engine=engine)
    if not user:
        return None
    if not verify_password(plain_password, user.password):
        return None
    return user


async def read_user_by_id(user_id: str, engine: DatabaseEngineAsync) -> UserInDBModel | None:
    async with engine.session() as session:
        stmt = select(User).filter_by(user_id=user_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return UserInDBModel(**result.__dict__)
    return None


async def read_users_by_ids(user_ids: list[str], engine: DatabaseEngineAsync) -> list[UserInDBModel] | None:
    async with engine.session() as session:
        stmt = select(User).filter(User.user_id.in_(user_ids))
        result = (await session.execute(stmt)).scalars().all()
        if result is not None:
            return [UserInDBModel(**res.__dict__) for res in result]


async def read_user_by_name(username: str, engine: DatabaseEngineAsync) -> UserInDBModel | None:
    async with engine.session() as session:
        stmt = select(User).filter_by(username=username)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return UserInDBModel(**result.__dict__)
    return None


async def read_users(engine: DatabaseEngineAsync,
                     project_id: str | None = None,
                     order_by_username: bool = False) -> list[UserInDBModel] | None:
    """
    Returns a list of all users (if `project_id` is None) or a list of users that
    are part of a project (have an existing `project_permission` with that `project_id`).

    Optionally, the results will be ordered by username.

    :param engine: async db engine
    :param project_id: If not None, results will be filtered to users in this project
    :param order_by_username: If true, results will be ordered by username
    :return: List of users or None (if applied filter has no response)
    """
    async with engine.session() as session:
        stmt = select(User)

        if project_id is not None:
            stmt.join(ProjectPermissions, ProjectPermissions.user_id == User.user_id)
            stmt.where(ProjectPermissions.project_id == project_id)

        if order_by_username:
            stmt.order_by(asc(User.username))

        result = (await session.execute(stmt)).scalars().all()
        if result is not None:
            return [UserInDBModel(**res.__dict__) for res in result]


async def create_or_update_user(user: UserModel | UserInDBModel, engine: DatabaseEngineAsync) -> str:
    """
    This updates or saves a user.
    Note, that `user_id` and `username` are not editable by this function. This is by design.

    - If `user_id` is empty, one will be added.
    - Password will only be updated in the DB if field is not None.
    - Password is assumed to be plaintext at this point (yolo) and will be hashed internally.

    :param user: user information
    :param engine: async db engine
    :return: Returns the `user_id` as string.
    """

    async with engine.session() as session:  # type: AsyncSession
        user_db: User | None = (
            await session.execute(select(User).where(User.user_id == user.user_id))
        ).scalars().one_or_none()

        if user_db is None:  # seems to be a new user
            if user.user_id is None:
                user_id = str(uuid4())
                user.user_id = user_id
            session.add(User(**user.dict()))
        else:
            # user_id -> not editable
            # username -> not editable
            user_db.email = user.email
            user_db.full_name = user.full_name
            user_db.affiliation = user.affiliation
            user_db.is_active = user.is_active
            user_db.is_superuser = user.is_superuser

            password: str | None = getattr(user, 'password', None)
            if password is not None:
                user_db.password = get_password_hash(password)

            user_id = str(user_db.user_id)

        # save changes
        await session.commit()
        return user_id
