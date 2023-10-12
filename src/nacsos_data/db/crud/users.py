import uuid
from uuid import uuid4
from typing import TYPE_CHECKING

from sqlalchemy import select, asc
from passlib.context import CryptContext

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import User, ProjectPermissions
from nacsos_data.models.users import UserInDBModel, UserModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401

pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


async def read_user_by_id(user_id: str | uuid.UUID, engine: DatabaseEngineAsync) -> UserInDBModel | None:
    session: AsyncSession
    async with engine.session() as session:
        stmt = select(User).filter_by(user_id=user_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return UserInDBModel(**result.__dict__)
    return None


async def read_users_by_ids(user_ids: list[str], engine: DatabaseEngineAsync) -> list[UserInDBModel] | None:
    session: AsyncSession
    async with engine.session() as session:
        stmt = select(User).filter(User.user_id.in_(user_ids))
        result = (await session.execute(stmt)).scalars().all()
        if result is not None and len(result) > 0:
            return [UserInDBModel(**res.__dict__) for res in result]

    return None


async def read_user_by_name(username: str, engine: DatabaseEngineAsync) -> UserInDBModel | None:
    session: AsyncSession
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
    session: AsyncSession
    async with engine.session() as session:
        stmt = select(User)

        if project_id is not None:
            stmt = stmt.join(ProjectPermissions, ProjectPermissions.user_id == User.user_id)
            stmt = stmt.where(ProjectPermissions.project_id == project_id)

        if order_by_username:
            stmt = stmt.order_by(asc(User.username))

        result = (await session.scalars(stmt)).all()
        if result is not None and len(result) > 0:
            return [UserInDBModel(**res.__dict__) for res in result]

    return None


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

    session: AsyncSession
    async with engine.session() as session:
        user_db: User | None = (
            await session.execute(select(User).where(User.user_id == user.user_id))
        ).scalars().one_or_none()

        password: str | None = getattr(user, 'password', None)
        hashed_password: str | None = None
        if password is not None:
            hashed_password = get_password_hash(password)

        if user_db is None:  # seems to be a new user
            user_id: str
            if user.user_id is None:
                user_id = str(uuid4())  # type: ignore[unreachable]
                user.user_id = user_id
            else:
                user_id = str(user.user_id)

            if hashed_password is None:
                raise ValueError('Missing password!')
            setattr(user, 'password', hashed_password)
            session.add(User(**user.model_dump()))
        else:
            # user_id -> not editable
            # username -> not editable
            user_db.email = user.email
            user_db.full_name = user.full_name
            user_db.affiliation = user.affiliation
            user_db.is_active = user.is_active
            user_db.is_superuser = user.is_superuser

            if hashed_password is not None:
                user_db.password = hashed_password

            user_id = str(user_db.user_id)

        # save changes
        await session.commit()
        return user_id
