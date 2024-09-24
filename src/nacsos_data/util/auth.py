import uuid
import logging
import datetime
from collections import defaultdict
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


class AuthenticationCache:
    def __init__(self, engine: DatabaseEngineAsync, refresh_time: int = 600):
        self.db_engine = engine
        self.refresh_time = refresh_time

        self._permission_cache: dict[str, dict[str, ProjectPermissionsModel]] = {}  # dict[project_id, dict[user_id, Permission]]
        self._user_cache: dict[str, UserModel] = {}  # dict[user_id, User]
        self._token_cache: dict[str, AuthTokenModel] = {}  # dict[token_id, Token]

        self._user_lookup: dict[str, str] = {}  # dict[username, user_id]
        self._token_lookup: dict[str, str] = {}  # dict[username, token_id]

    async def init(self) -> None:
        logger.info('Initialising auth cache')
        await self.reload_all()

    def __await__(self) -> Generator[Any, Any, Any]:
        return self.init().__await__()

    async def reload_all(self) -> None:
        await self.reload_permissions()
        await self.reload_users()
        await self.reload_tokens()

    async def reload_permissions(self) -> None:
        logger.info('Refreshing permissions cache')
        self._permission_cache = defaultdict(dict)
        async with self.db_engine.engine.connect() as conn:
            rslt = await conn.execute(select(ProjectPermissions))
            for row in rslt.mappings().all():
                perm = ProjectPermissionsModel.model_validate(row)
                self._permission_cache[str(perm.project_id)][str(perm.user_id)] = perm
        logger.debug(f'Known project_ids: {self._permission_cache.keys()}')

    async def reload_users(self) -> None:
        logger.info('Refreshing user cache')
        self._user_cache = {}
        self._user_lookup = {}
        async with self.db_engine.engine.connect() as conn:
            rslt = await conn.execute(select(User))
            for row in rslt.mappings().all():
                user = UserModel.model_validate(row)
                self._user_cache[str(user.user_id)] = user
                self._user_lookup[str(user.username)] = str(user.user_id)

        logger.debug(f'Known user_ids: {self._user_cache.keys()}')
        logger.debug(f'Known usernames: {self._user_lookup.keys()}')

    async def reload_tokens(self) -> None:
        logger.info('Refreshing token cache')
        self._token_cache = {}
        self._token_lookup = {}
        async with self.db_engine.engine.connect() as conn:
            rslt = await conn.execute(select(AuthToken)
                                      .where(or_(AuthToken.valid_till > datetime.datetime.now(),
                                                 AuthToken.valid_till.is_(None))))
            for row in rslt.mappings().all():
                token = AuthTokenModel.model_validate(row)
                self._token_cache[str(token.token_id)] = token
                self._token_lookup[token.username] = str(token.token_id)

        logger.debug(f'Known token_ids: {self._token_cache.keys()}')
        logger.debug(f'Known token usernames: {self._token_lookup.keys()}')

    async def get_user(self, username: str | None = None,
                       user_id: str | uuid.UUID | None = None,
                       user: UserModel | None = None,
                       retry: bool = False) -> UserModel:
        if user is not None:
            return user
        try:
            if user_id is not None:
                return self._user_cache[str(user_id).lower().strip()]

            if username is not None:
                return self._user_cache[self._user_lookup[username.lower().strip()]]
        except KeyError as e:
            logger.error(f'Did not find "{username}" / "{user_id}" in the list of {self._token_lookup}')
            if not retry:
                logger.warning('Did not find requested user, trying to reload!')
                await self.reload_users()
                return await self.get_user(username=username, user_id=user_id, user=user, retry=True)

            raise e

        raise RuntimeError('Need to specify either username or user_id or user!')

    async def get_project_permission(self,
                                     project_id: str | uuid.UUID,
                                     username: str | None = None,
                                     user_id: str | uuid.UUID | None = None,
                                     user: UserModel | None = None,
                                     retry: bool = False) -> ProjectPermissionsModel | None:
        user = await self.get_user(username=username, user_id=user_id, user=user)
        permission = self._permission_cache.get(str(project_id).lower().strip(), {}).get(str(user.user_id).lower().strip())

        if permission is None and not retry:
            logger.warning('Did not find requested project-user permission, trying to reload!')
            await self.reload_permissions()
            return await self.get_project_permission(project_id=project_id, username=username, user_id=user_id, user=user, retry=True)

        return permission

    async def get_auth_token(self,
                             token_id: str | uuid.UUID | None = None,
                             username: str | None = None,
                             user_id: str | uuid.UUID | None = None,
                             user: UserModel | None = None,
                             retry: bool = False) -> AuthTokenModel | None:
        if token_id is not None:
            tok = self._token_cache.get(str(token_id).lower().strip())
            if tok:
                return tok

        try:
            user = await self.get_user(username, user_id, user)
            token_id = self._token_lookup[str(user.username).lower().strip()]  # type: ignore[index,union-attr]
            return self._token_cache[str(token_id).lower().strip()]
        except Exception as e:
            if not retry:
                logger.warning('Did not find requested token, trying to reload!')
                await self.reload_tokens()
                return await self.get_auth_token(token_id=token_id, username=username, user_id=user_id, user=user, retry=True)

            logger.error(f'Did not find "{user.username}" in the list of {self._token_lookup}')  # type: ignore[union-attr]
            raise e


class Authentication:
    def __init__(self,
                 engine: DatabaseEngineAsync,
                 token_lifetime_minutes: int = 5,
                 default_user: str | None = None):
        """

        :param engine:
        :param token_lifetime_minutes: Time before token becomes obsolete
        :param default_user: username (NOT uuid or email!) in database of a user;
                             if not None, authentication is skipped and the rest is handled as this user
        """
        self.token_lifetime_minutes = token_lifetime_minutes
        self.default_user = default_user
        self.db_engine = engine
        self.cache = AuthenticationCache(engine)

    async def init(self) -> None:
        logger.info('Initialising auth helper')
        await self.cache

    def __await__(self) -> Generator[Any, Any, Any]:
        return self.init().__await__()

    async def check_username_password(self,
                                      plain_password: str,
                                      username: str | None = None,
                                      user_id: str | uuid.UUID | None = None) -> UserModel:
        user = await self.cache.get_user(username=username, user_id=user_id)
        async with self.db_engine.engine.connect() as conn:
            passwd = (
                await conn.execute(
                    select(User.password).where(or_(User.username == username,
                                                    User.user_id == user_id)))
            ).mappings().one_or_none()

        if not verify_password(plain_password=plain_password, hashed_password=passwd['password']):  # type: ignore[index]
            raise InvalidCredentialsError('username/user_id or password wrong!')

        return user

    async def fetch_token_by_user(self, username: str, only_active: bool = True) -> AuthTokenModel:
        token = await self.cache.get_auth_token(username=username)
        if token is not None and (
                not only_active or (only_active and (token.valid_till is None or token.valid_till > datetime.datetime.now()))):
            return token
        raise InvalidCredentialsError(f'No valid auth token found for user "{username}"')

    async def fetch_token_by_id(self, token_id: str | uuid.UUID, only_active: bool = True) -> AuthTokenModel:
        token = await self.cache.get_auth_token(token_id=token_id)
        if token is not None and (
                not only_active or (only_active and (token.valid_till is None or token.valid_till > datetime.datetime.now()))):
            return token
        raise InvalidCredentialsError('No valid auth token found for this ID')

    async def clear_tokens_inactive(self) -> None:
        async with self.db_engine.engine.connect() as conn:  # type: AsyncConnection
            await conn.execute(delete(AuthToken).where(AuthToken.valid_till < datetime.datetime.now()))
            await conn.commit()
        await self.cache.reload_tokens()

    async def clear_tokens_by_user(self, username: str) -> None:
        async with self.db_engine.engine.connect() as conn:  # type: AsyncConnection
            await conn.execute(delete(AuthToken).where(AuthToken.username == username))
            await conn.commit()
        await self.cache.reload_tokens()

    async def clear_token_by_id(self,
                                token_id: str | uuid.UUID,
                                verify_username: str | None = None) -> None:
        async with self.db_engine.engine.connect() as conn:  # type: AsyncConnection
            stmt = delete(AuthToken).where(AuthToken.token_id == token_id)
            if verify_username:
                stmt = stmt.where(AuthToken.username == verify_username)
            await conn.execute(stmt)
            await conn.commit()
        await self.cache.reload_tokens()

    async def refresh_or_create_token(self,
                                      username: str | None = None,
                                      token_id: str | uuid.UUID | None = None,
                                      token_lifetime_minutes: int | None = None,
                                      verify_username: str | None = None) -> AuthTokenModel:
        if token_lifetime_minutes is None:
            token_lifetime_minutes = self.token_lifetime_minutes

        session: AsyncSession
        async with self.db_engine.session() as session:
            valid_till = datetime.datetime.now() + datetime.timedelta(minutes=token_lifetime_minutes)
            if token_id is not None:
                stmt = select(AuthToken).where(AuthToken.token_id == token_id)
            elif username is not None:
                stmt = select(AuthToken).where(AuthToken.username == username)
            else:
                raise AssertionError('Missing username or token_id!')

            token_orm: AuthToken | None = (await session.scalars(stmt)).one_or_none()

            # There's an existing token that we just need to update
            if token_orm is not None:
                if verify_username is not None and verify_username != token_orm.username:
                    raise InvalidCredentialsError('This is not you!')

                token_orm.valid_till = valid_till
                token = AuthTokenModel.model_validate(token_orm.__dict__)
                await session.commit()
                await self.cache.reload_tokens()
                return token

            # No token exists yet, but username was provided, so we create one
            elif token_orm is None and username is not None:
                token_id = uuid.uuid4()
                token = AuthTokenModel(token_id=token_id, username=username, valid_till=valid_till)
                token_orm = AuthToken(**token.model_dump())
                session.add(token_orm)
                await session.commit()
                await self.cache.reload_tokens()
                return token

            # Failed!
            else:
                raise InvalidCredentialsError(f'No auth token found for {username} / {token_id}!')

    async def get_current_user(self, token_id: str | uuid.UUID) -> UserModel:
        token = await self.cache.get_auth_token(token_id=token_id)

        if token is None:
            raise InvalidCredentialsError(f'No token with id={token_id}!')

        user = await self.cache.get_user(username=token.username)

        logger.debug(f'Current user: {user.username} ({user.user_id})')
        return user

    async def get_project_permissions(self,
                                      project_id: str | uuid.UUID,
                                      username: str | None = None,
                                      user_id: str | None = None,
                                      user: UserModel | None = None) -> ProjectPermissionsModel:
        if username is not None or user_id is not None:
            user = await self.cache.get_user(username=username, user_id=user_id)

        if user is None:
            raise RuntimeError()  # should never happen, just for mypy

        if user.user_id is None:
            raise RuntimeError('Inconsistent behaviour, user is missing an ID!')

        if user.is_superuser:
            logger.debug('Using super_admin permissions!')
            # admin gets to do anything always, so return with simulated full permissions
            return ProjectPermissionsModel.get_virtual_admin(project_id=project_id, user_id=user.user_id)

        permissions = await self.cache.get_project_permission(project_id=project_id, user=user)

        if permissions is None:
            raise InsufficientPermissionError('No permission found for this project!')

        return permissions

    async def check_permissions(self,
                                project_id: str | uuid.UUID,
                                user_id: str | uuid.UUID | None = None,
                                username: str | None = None,
                                user: UserModel | None = None,
                                required_permissions: list[ProjectPermission] | ProjectPermission | None = None,
                                fulfill_all: bool = True) -> UserPermissions:
        if user_id is None and username is None and user is None:
            raise AssertionError('Need one of `user_id`, `username` or `user`!')

        # we did not get a user object, so we have to fetch the user info from the database
        if user is None:
            user = await self.cache.get_user(username=username, user_id=user_id)

        if type(required_permissions) is str:
            required_permissions = [required_permissions]

        permissions = await self.get_project_permissions(project_id=project_id, user=user)
        user_permissions = UserPermissions(user=user, permissions=permissions)

        # no specific permissions were required (only basic access to the project) -> permitted!
        if required_permissions is None:
            return user_permissions

        any_permission_fulfilled = False

        # check that each required permission is fulfilled
        for permission in required_permissions:
            p_permission = getattr(permissions, permission, False)
            if fulfill_all and not p_permission:
                raise InsufficientPermissionError(f'User does not have permission "{permission}" '
                                                  f'for project "{project_id}".')
            any_permission_fulfilled = any_permission_fulfilled or p_permission

        if not any_permission_fulfilled and not fulfill_all:
            raise InsufficientPermissionError(
                f'User does not have any of the required permissions ({required_permissions}) '
                f'for project "{project_id}".'
            )
        return user_permissions


__all__ = ['UserPermissions', 'InvalidCredentialsError', 'InsufficientPermissionError', 'Authentication']
