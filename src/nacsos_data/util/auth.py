import uuid
import logging
import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy import select, delete, or_

from ..db import DatabaseEngineAsync
from ..db.crud.projects import read_project_permissions_for_user
from ..db.crud.users import read_user_by_name, read_user_by_id, verify_password
from ..db.schemas.users import AuthToken
from ..models.projects import ProjectPermissionsModel, ProjectPermission
from ..models.users import UserInDBModel, UserModel, AuthTokenModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401

logger = logging.getLogger('nacsos_data.util.auth')


class UserPermissions(BaseModel):
    user: UserModel
    permissions: ProjectPermissionsModel


class InvalidCredentialsError(PermissionError):
    pass


class InsufficientPermissionError(PermissionError):
    pass


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

    async def check_username_password(self,
                                      plain_password: str,
                                      username: str | None = None,
                                      user_id: str | None = None) -> UserInDBModel:
        user: UserInDBModel | None = None
        if username is not None:
            user = await read_user_by_name(username=username, engine=self.db_engine)
        elif user_id is not None:
            user = await read_user_by_id(user_id=user_id, engine=self.db_engine)

        if user is None:
            raise InvalidCredentialsError('No user found for username/user_id!')

        if not verify_password(plain_password=plain_password, hashed_password=user.password):
            raise InvalidCredentialsError('username/user_id or password wrong!')

        return user

    async def fetch_token_by_user(self, username: str, only_active: bool = True) -> AuthTokenModel:
        session: AsyncSession
        async with self.db_engine.session() as session:
            stmt = select(AuthToken).where(AuthToken.username == username)
            if only_active:
                stmt = stmt.where(or_(AuthToken.valid_till > datetime.datetime.now(),
                                      AuthToken.valid_till.is_(None)))
            token = (await session.scalars(stmt)).one_or_none()

            if token is None:
                raise InvalidCredentialsError(f'No valid auth token found for user "{username}"')
            return AuthTokenModel.model_validate(token.__dict__)

    async def fetch_token_by_id(self, token_id: str | uuid.UUID, only_active: bool = True) -> AuthTokenModel:
        session: AsyncSession
        async with self.db_engine.session() as session:
            stmt = select(AuthToken).where(AuthToken.token_id == token_id)
            if only_active:
                stmt = stmt.where(or_(AuthToken.valid_till > datetime.datetime.now(),
                                      AuthToken.valid_till.is_(None)))
            token = (await session.scalars(stmt)).one_or_none()

            if token is None:
                raise InvalidCredentialsError(f'No valid auth token found for token_id "{token_id}"')

            return AuthTokenModel.model_validate(token.__dict__)

    async def clear_tokens_inactive(self) -> None:
        session: AsyncSession
        async with self.db_engine.session() as session:
            stmt = delete(AuthToken).where(AuthToken.valid_till < datetime.datetime.now())
            await session.execute(stmt)

    async def clear_tokens_by_user(self, username: str) -> None:
        session: AsyncSession
        async with self.db_engine.session() as session:
            stmt = delete(AuthToken).where(AuthToken.username == username)
            await session.execute(stmt)
            await session.commit()

    async def clear_token_by_id(self,
                                token_id: str | uuid.UUID,
                                verify_username: str | None = None) -> None:
        session: AsyncSession
        async with self.db_engine.session() as session:
            stmt = delete(AuthToken).where(AuthToken.token_id == token_id)
            if verify_username:
                stmt = stmt.where(AuthToken.username == verify_username)
            await session.execute(stmt)
            await session.commit()

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
                raise InvalidCredentialsError('No auth token found!')

    async def get_current_user(self, token_id: str | uuid.UUID | None = None) -> UserInDBModel:

        if self.default_user:
            username = self.default_user
            logger.warning(f'Authentication using fake user: {self.default_user}!')
        else:
            if token_id is None:
                raise InvalidCredentialsError('Empty auth token!')
            token = await self.fetch_token_by_id(token_id=token_id, only_active=True)
            username = token.username

        user = await read_user_by_name(username=username, engine=self.db_engine)

        if user is None:
            raise InvalidCredentialsError('No user found for username!')

        logger.debug(f'Current user: {user.username} ({user.user_id})')
        return user

    async def get_project_permissions(self,
                                      project_id: str | uuid.UUID,
                                      user: UserInDBModel) -> ProjectPermissionsModel:
        user_id = user.user_id
        if user_id is None:
            raise RuntimeError('Inconsistent behaviour, user is missing an ID!')

        if user.is_superuser:
            # admin gets to do anything always, so return with simulated full permissions
            return ProjectPermissionsModel.get_virtual_admin(project_id=str(project_id),
                                                             user_id=str(user_id))

        permissions = await read_project_permissions_for_user(user_id=user_id,
                                                              project_id=project_id,
                                                              engine=self.db_engine)
        if permissions is None:
            raise InsufficientPermissionError('No permission found for this project!')

        return permissions

    async def check_permissions(self,
                                project_id: str | uuid.UUID,
                                user_id: str | uuid.UUID | None = None,
                                username: str | None = None,
                                user: UserModel | UserInDBModel | None = None,
                                required_permissions: list[ProjectPermission] | ProjectPermission | None = None,
                                fulfill_all: bool = True) -> UserPermissions:
        if user_id is None and username is None and user is None:
            raise AssertionError('Need one of `user_id`/`username` or `user`!')

        # we did not get a user object, so we have to fetch the user info from the database
        if user is None:
            if username is not None:
                user = await read_user_by_name(username=username, engine=self.db_engine)
            elif user_id is not None:
                user = await read_user_by_id(user_id=user_id, engine=self.db_engine)
            else:
                raise RuntimeError('Implausible state: `username` and `user_id` is None!')

            # double-check that we actually found a user
            if user is None:
                raise InvalidCredentialsError('No user found for this username/user_id!')

        if type(required_permissions) is str:
            required_permissions = [required_permissions]

        permissions = await self.get_project_permissions(project_id=project_id,
                                                         user=user)  # type: ignore[arg-type]
        user_permissions = UserPermissions(user=UserModel.model_validate(user.model_dump()), permissions=permissions)

        # no specific permissions were required (only basic access to the project) -> permitted!
        if required_permissions is None:
            return user_permissions

        any_permission_fulfilled = False

        # check that each required permission is fulfilled
        for permission in required_permissions:
            p_permission = getattr(permissions, permission, False)
            if fulfill_all and not p_permission:
                raise InsufficientPermissionError(
                    f'User does not have permission "{permission}" for project "{project_id}".'
                )
            any_permission_fulfilled = any_permission_fulfilled or p_permission

        if not any_permission_fulfilled and not fulfill_all:
            raise InsufficientPermissionError(
                f'User does not have any of the required permissions ({required_permissions}) '
                f'for project "{project_id}".'
            )

        return user_permissions


__all__ = ['UserPermissions', 'InvalidCredentialsError', 'InsufficientPermissionError', 'Authentication']
