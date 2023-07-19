import json
import logging
from typing import AsyncIterator, Iterator, Any
from json import JSONEncoder

from pydantic import BaseModel
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
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
        if type(o) == datetime:
            return o.strftime('%Y-%m-%dT%H:%M:%S')

        # Translate pydantic models into dict
        if isinstance(o, BaseModel):
            return o.dict()

        return json.JSONEncoder.default(self, o)


class DatabaseEngineAsync:
    """
    This class is the main entry point to access the database.
    It handles the connection, engine, and session.
    """

    def __init__(self, host: str, port: int, user: str, password: str,
                 database: str = 'nacsos_core', debug: bool = False):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

        # TODO expire_on_commit (check if this should be turned off)
        self._connection_str = URL.create(
            drivername='postgresql+psycopg',
            username=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
            database=self._database,
        )
        self.engine = create_async_engine(self._connection_str, echo=debug, future=True,
                                          json_serializer=DictLikeEncoder().encode)
        self._session: async_sessionmaker[AsyncSession] = async_sessionmaker(  # type: ignore[type-arg] # FIXME
            bind=self.engine, autoflush=False, autocommit=False)

    async def startup(self) -> None:
        """
        Call this function to initialise the database engine.
        """
        try:
            logger.debug('AsyncEngine starting up...')
            logger.info(f'AsyncEngine connecting to {self._user}:****@{self._host}:{self._port}/{self._database}')
            session: AsyncSession = self._session()
            await session.execute(text('SELECT 1;'))
            logger.info('Connection seems to be ready.')
        except OperationalError as e:
            logger.error('Connection failed!')
            logger.exception(e)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        session: AsyncSession = self._session()
        try:
            yield session
            await session.commit()  # FIXME should there even be a commit always?
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

    def __init__(self, host: str, port: int, user: str, password: str,
                 database: str = 'nacsos_core', debug: bool = False):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

        # TODO expire_on_commit (check if this should be turned off)
        self._connection_str = URL.create(
            drivername='postgresql+psycopg',
            username=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
            database=self._database,
        )
        self.engine = create_engine(self._connection_str, echo=debug, future=True,
                                    json_serializer=DictLikeEncoder().encode)
        self._session: sessionmaker[Session] = sessionmaker(  # type: ignore[type-arg] # FIXME
            bind=self.engine, autoflush=False, autocommit=False)

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
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
