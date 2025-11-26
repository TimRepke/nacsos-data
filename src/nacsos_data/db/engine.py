import json
import logging
from functools import wraps
from pathlib import Path
from typing import AsyncIterator, Iterator, Any, TypeVar, Callable, Awaitable, Coroutine, TypeAlias, Type
from json import JSONEncoder

from pydantic import BaseModel
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession, AsyncConnection
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine, text, URL
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime

# unused import required so the engine sees the models!
from . import schemas  # noqa F401

logger = logging.getLogger('nacsos_data.engine')


class DictLikeEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        # Translate datetime into a string
        if isinstance(o, datetime):
            return o.strftime('%Y-%m-%dT%H:%M:%S')

        # Translate Path into a string
        if isinstance(o, Path):
            return str(o)

        # Translate pydantic models into dict
        if isinstance(o, BaseModel):
            return o.model_dump()

        return json.JSONEncoder.default(self, o)


class DatabaseEngineAsync:
    """
    This class is the main entry point to access the database.
    It handles the connection, engine, and session.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str = 'nacsos_core',
        debug: bool = False,
        kw_engine: dict[str, Any] | None = None,
        kw_session: dict[str, Any] | None = None,
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

        self._connection_str = URL.create(
            drivername='postgresql+psycopg',
            username=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
            database=self._database,
        )
        self.engine = create_async_engine(
            self._connection_str,
            **{
                'echo': debug,
                'future': True,
                'json_serializer': DictLikeEncoder().encode,
                **(kw_engine or {}),
            },
        )
        self._session: async_sessionmaker[AsyncSession] = async_sessionmaker(
            **{
                'bind': self.engine,
                'autoflush': False,
                'autocommit': False,
                'expire_on_commit': True,
                'class_': DBSession,
                **(kw_session or {}),
            }
        )

    async def startup(self) -> None:
        """
        Call this function to initialise the database engine.
        """
        try:
            logger.debug('AsyncEngine starting up...')
            logger.info(f'AsyncEngine connecting to {self._user}:****@{self._host}:{self._port}/{self._database}')
            async with self._session() as session:
                await session.execute(text('SELECT 1;'))
                logger.info('Connection seems to be ready.')
        except OperationalError as e:
            logger.error('Connection failed!')
            logger.exception(e)

    @asynccontextmanager
    async def session(self, use_commit: bool = False) -> AsyncIterator[AsyncSession]:
        session: AsyncSession = self._session(use_commit=use_commit)

        if logger.isEnabledFor(logging.DEBUG):
            import inspect

            curframe = inspect.currentframe()
            calframe = inspect.getouterframes(curframe, 2)
            logger.debug('New session for: ' + ' <- '.join([fr[3] for fr in calframe[2:6]]))

        try:
            yield session
            # await session.commit()
        except Exception as e:
            await session.rollback()
            raise e
        finally:
            await session.close()


class DatabaseEngine:
    """
    This class is the main entry point to access the database.
    It handles the connection, engine, and session.
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str = 'nacsos_core', debug: bool = False):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

        self._connection_str = URL.create(
            drivername='postgresql+psycopg',
            username=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
            database=self._database,
        )
        self.engine = create_engine(self._connection_str, echo=debug, future=True, json_serializer=DictLikeEncoder().encode)
        self._session: sessionmaker[Session] = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

    def startup(self) -> None:
        """
        Call this function to initialise the database engine.
        """
        # SQLModel.metadata.create_all(self.engine)
        pass

    def __call__(self, *args: tuple[Any, ...], **kwargs: dict[str, Any]) -> Session:
        return self._session()

    @contextmanager
    def session(self) -> Iterator[Session]:
        # https://rednafi.github.io/digressions/python/2020/03/26/python-contextmanager.html
        session = self._session()
        try:
            yield session
            # session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()


R = TypeVar('R')
CommitFunc: TypeAlias = Callable[[], Coroutine[None, None, None]]


def flush_or_commit(session: AsyncSession, use_commit: bool) -> CommitFunc:
    async def func() -> None:
        if use_commit:
            await session.commit()
        else:
            await session.flush()

    return func


class DBSession(AsyncSession):
    def __init__(
        self,
        bind: Any | None = None,
        *,
        binds: dict[str, Any] | None = None,
        sync_session_class: Type[Session] | None = None,
        use_commit: bool = False,
        **kw: Any,
    ):
        super().__init__(bind=bind, binds=binds, sync_session_class=sync_session_class)  # type: ignore[arg-type]
        self.use_commit = use_commit

    async def flush_or_commit(self) -> None:
        if self.use_commit:
            await self.commit()
        else:
            await self.flush()


DBConnection: TypeAlias = AsyncConnection | AsyncSession | DBSession


def ensure_connection_async(func: Callable[..., Awaitable[R]]) -> Callable[..., Awaitable[R]]:
    @wraps(func)
    async def wrapper(*args: Any, connection: DatabaseEngineAsync | AsyncConnection | AsyncSession | DBSession, **kwargs: dict[str, Any]) -> R:
        if isinstance(connection, AsyncConnection):
            return await func(*args, db_conn=connection, **kwargs)

        conn: AsyncConnection
        if isinstance(connection, DatabaseEngineAsync):
            async with connection.engine.connect() as conn:
                return await func(*args, db_conn=conn, **kwargs)

        if isinstance(connection, DBSession) or isinstance(connection, AsyncSession):
            try:
                conn = await connection.connection()
                ret = await func(*args, db_conn=conn, **kwargs)
            except Exception as e:
                await conn.rollback()
                raise e
            finally:
                await conn.close()
                return ret  # noqa: B012

        raise RuntimeError('Unsupported connection type!')

    return wrapper


def ensure_session_async(func: Callable[..., Awaitable[R]]) -> Callable[..., Awaitable[R]]:
    @wraps(func)
    async def wrapper(
        *args: Any,
        session: AsyncSession | DBSession | None = None,
        db_engine: DatabaseEngineAsync | None = None,
        engine: DatabaseEngineAsync | None = None,
        use_commit: bool = False,
        **kwargs: dict[str, Any],
    ) -> R:
        if session is not None:
            # FIXME: In theory, we could give this a normal AsyncSession and a func using `session.flush_or_commit()` would blow up.
            # if isinstance(session, AsyncSession):
            #     await func(*args, session=DBSession(session=fresh_session, use_commit=use_commit), **kwargs)
            return await func(*args, session=session, **kwargs)

        if engine is not None:
            db_engine = engine  # alias; fall through and use the other branch to ensure session

        if db_engine is not None:
            async with db_engine.session(use_commit=use_commit) as session:
                return await func(*args, session=session, **kwargs)

        raise RuntimeError('I need a session or an engine to get a session!')

    return wrapper


def ensure_session(func):  # type: ignore[no-untyped-def]
    @wraps(func)
    def wrapper(
        *args,  # type: ignore[no-untyped-def]
        session: Session | None = None,
        db_engine: DatabaseEngine | None = None,
        **kwargs,
    ):
        if session is not None:
            return func(*args, session=session, **kwargs)
        if db_engine is not None:
            logger.debug(f'Opening a new session to execute {func}')
            fresh_session: Session
            with db_engine.session() as fresh_session:
                return func(*args, session=fresh_session, **kwargs)

        raise RuntimeError('I need a session or an engine to get a session!')

    return wrapper
