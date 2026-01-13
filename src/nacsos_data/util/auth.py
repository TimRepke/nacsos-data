import uuid
import logging
import datetime
from typing import TYPE_CHECKING, Generator, Any
from pydantic import BaseModel
from sqlalchemy import select, delete, or_

from ..db import DatabaseEngineAsync
from ..db.crud.users import verify_password
from ..db.schemas import ProjectPermissions
from ..db.schemas.users import AuthToken, User
from ..models.projects import ProjectPermissionsModel, ProjectPermission
from ..models.users import UserModel, AuthTokenModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, AsyncConnection  # noqa: F401

logger = logging.getLogger('nacsos_data.util.auth')


class UserPermissions(BaseModel):
    user: UserModel
    permissions: ProjectPermissionsModel


class InvalidCredentialsError(PermissionError):
    pass


class InsufficientPermissionError(PermissionError):
    pass


class Authentication:
    def __init__(self, engine: DatabaseEngineAsync, token_lifetime_minutes: int = 5, default_user: str | None = None):
        """

        :param engine:
        :param token_lifetime_minutes: Time before token becomes obsolete
        :param default_user: username (NOT uuid or email!) in database of a user;
                             if not None, authentication is skipped and the rest is handled as this user
        """
        self.token_lifetime_minutes = token_lifetime_minutes
        self.default_user = default_user
        self.db_engine = engine

    async def init(self) -> None:
        logger.info('Initialising auth helper')

    def __await__(self) -> Generator[Any, Any, Any]:
        return self.init().__await__()

    async def check_password(self, plain_password: str, username: str | None = None, user_id: str | uuid.UUID | None = None) -> UserModel:
        if username is None and user_id is None:
            raise InvalidCredentialsError('Need to specify either username or user_id!')

        async with self.db_engine.engine.connect() as conn:  # type: AsyncConnection
            user = (await conn.execute(select(User).where(or_(User.username == username, User.user_id == user_id)))).mappings().one_or_none()
        if not user or 'password' not in user or not verify_password(plain_password=plain_password, hashed_password=user['password']):
            raise InvalidCredentialsError('username, user_id, or password wrong!')
        return UserModel.model_validate(user)

    async def fetch_token(self, username: str | uuid.UUID | None = None, token_id: str | uuid.UUID | None = None, only_active: bool = True) -> AuthTokenModel:
        if username is None and token_id is None:
            raise InvalidCredentialsError('Need to specify either username or token_id!')

        async with self.db_engine.engine.connect() as conn:  # type: AsyncConnection
            where = [or_(AuthToken.username == username, AuthToken.token_id == token_id)]
            if only_active:
                where.append(or_(AuthToken.valid_till == None, AuthToken.valid_till > datetime.datetime.now()))  # noqa: E711
            token = (await conn.execute(select(AuthToken).where(*where))).mappings().one_or_none()
        if token:
            return AuthTokenModel.model_validate(token)
        raise InvalidCredentialsError(f'No valid auth token found for user "{username}" or token_id {token_id}!')

    async def clear_tokens_inactive(self) -> None:
        async with self.db_engine.engine.connect() as conn:  # type: AsyncConnection
            await conn.execute(delete(AuthToken).where(AuthToken.valid_till < datetime.datetime.now()))
            await conn.commit()

    async def clear_tokens_by_user(self, username: str) -> None:
        async with self.db_engine.engine.connect() as conn:  # type: AsyncConnection
            await conn.execute(delete(AuthToken).where(AuthToken.username == username))
            await conn.commit()

    async def clear_token_by_id(self, token_id: str | uuid.UUID, verify_username: str | None = None) -> None:
        async with self.db_engine.engine.connect() as conn:  # type: AsyncConnection
            stmt = delete(AuthToken).where(AuthToken.token_id == token_id)
            if verify_username:
                stmt = stmt.where(AuthToken.username == verify_username)
            await conn.execute(stmt)
            await conn.commit()

    async def refresh_or_create_token(
        self,
        username: str | None = None,
        token_id: str | uuid.UUID | None = None,
        token_lifetime_minutes: int | None = None,
        verify_username: str | None = None,
    ) -> AuthTokenModel:
        if token_id is None and username is None:
            raise AssertionError('Missing username or token_id!')

        if token_lifetime_minutes is None:
            token_lifetime_minutes = self.token_lifetime_minutes

        session: AsyncSession
        async with self.db_engine.session() as session:
            valid_till = datetime.datetime.now() + datetime.timedelta(minutes=token_lifetime_minutes)

            stmt = select(AuthToken).where(or_(AuthToken.token_id == token_id, AuthToken.username == username))
            token_orm: AuthToken | None = (await session.scalars(stmt)).one_or_none()

            # There's an existing token that we just need to update
            if token_orm is not None:
                if verify_username is not None and verify_username != token_orm.username:
                    raise InvalidCredentialsError('This is not you!')

                token_orm.valid_till = valid_till
                token = AuthTokenModel.model_validate(token_orm.__dict__)
                await session.commit()
                return token

            # No token exists yet, but username was provided, so we create one
            elif token_orm is None and username is not None:
                token_id = uuid.uuid4()
                token = AuthTokenModel(token_id=token_id, username=username, valid_till=valid_till)
                token_orm = AuthToken(**token.model_dump())
                session.add(token_orm)
                await session.commit()
                return token

            # Failed!
            else:
                raise InvalidCredentialsError(f'No auth token found for {username} / {token_id}!')

    async def get_user(self, token_id: str | uuid.UUID | None = None, username: str | None = None, user_id: str | uuid.UUID | None = None) -> UserModel:
        async with self.db_engine.engine.connect() as conn:  # type: AsyncConnection
            user_orm = (
                (
                    await conn.execute(
                        select(User)
                        .join(AuthToken, AuthToken.username == User.username)
                        .where(
                            User.is_active == True,
                            or_(AuthToken.token_id == token_id, User.user_id == user_id, User.username == username),
                            or_(AuthToken.valid_till == None, AuthToken.valid_till > datetime.datetime.now()),  # noqa: E711
                        )
                        .limit(1),
                    )
                )
                .mappings()
                .one_or_none()
            )

        if not user_orm:
            raise InvalidCredentialsError(f'No user found for token: {token_id}!')

        user = UserModel.model_validate(user_orm)
        logger.debug(f'Current user: {user.username} ({user.user_id})')
        return user

    async def get_project_permissions(
        self,
        project_id: str | uuid.UUID,
        username: str | None = None,
        user_id: str | uuid.UUID | None = None,
        user: UserModel | None = None,
    ) -> ProjectPermissionsModel:
        if user is None and user_id is None and username is None:
            raise InsufficientPermissionError('Missing user or username or user_id!')

        if user and user.is_superuser and user.user_id:
            logger.debug('Using super_admin permissions!')
            # admin gets to do anything always, so return with simulated full permissions
            return ProjectPermissionsModel.get_virtual_admin(project_id=project_id, user_id=user.user_id)

        # if we get a user, override data
        if user is not None and (username is None and user_id is None):
            username = user.username if username is None else username
            user_id = user.user_id if user_id is None else user_id

        async with self.db_engine.engine.connect() as conn:  # type: AsyncConnection
            logger.debug(f'Checking user/project permissions for {username} ({user_id}) -> {project_id}...')
            permission_orm = (
                (
                    await conn.execute(
                        select(ProjectPermissions)
                        .join(User, ProjectPermissions.user_id == User.user_id)
                        .where(or_(ProjectPermissions.user_id == user_id, User.username == username), ProjectPermissions.project_id == project_id)
                    )
                )
                .mappings()
                .one_or_none()
            )
            if permission_orm:
                return ProjectPermissionsModel.model_validate(permission_orm)

            if user:
                raise InsufficientPermissionError('No permission found for this project and not superuser!')

            logger.debug('Checking if user is superuser...')
            user_orm = (
                (await conn.execute(select(User).where(or_(User.username == username, User.user_id == user_id, User.is_superuser == True)).limit(1)))
                .mappings()
                .one_or_none()
            )
            if user_orm:
                return ProjectPermissionsModel.get_virtual_admin(project_id=project_id, user_id=user_orm['user_id'])

        raise InsufficientPermissionError('No permission found for this project!')

    async def check_permissions(
        self,
        project_id: str | uuid.UUID,
        user_id: str | uuid.UUID | None = None,
        username: str | None = None,
        user: UserModel | None = None,
        required_permissions: list[ProjectPermission] | ProjectPermission | None = None,
        fulfill_all: bool = True,
    ) -> UserPermissions:
        if type(required_permissions) is str:
            required_permissions = [required_permissions]

        if not user:
            user = await self.get_user(user_id=user_id, username=username)
        if not user:  # redundant, just to appease mypy
            raise InsufficientPermissionError('No permission found for this user!')

        permissions = await self.get_project_permissions(project_id=project_id, user=user, username=username, user_id=user_id)
        user_permissions = UserPermissions(user=user, permissions=permissions)

        # no specific permissions were required (only basic access to the project) -> permitted!
        if required_permissions is None:
            return user_permissions

        any_permission_fulfilled = False

        # check that each required permission is fulfilled
        for permission in required_permissions:
            p_permission = getattr(permissions, permission, False)
            if fulfill_all and not p_permission:
                raise InsufficientPermissionError(f'User does not have permission "{permission}" for project "{project_id}".')
            any_permission_fulfilled = any_permission_fulfilled or p_permission

        if not any_permission_fulfilled and not fulfill_all:
            raise InsufficientPermissionError(f'User does not have any of the required permissions ({required_permissions}) for project "{project_id}".')
        return user_permissions


__all__ = ['UserPermissions', 'InvalidCredentialsError', 'InsufficientPermissionError', 'Authentication']
