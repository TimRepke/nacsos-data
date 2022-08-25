import json
from typing import AsyncIterator, Iterator, Any
from json import JSONEncoder
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime

# unused import required so the engine sees the models!
from . import schemas  # noqa F401


class DictLikeEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if type(o) == datetime:
            return o.strftime('%Y-%m-%dT%H:%M:%S')

        return json.JSONEncoder.default(self, o)


class DatabaseEngineAsync:
    """
    This class is the main entry point to access the database.
    It handles the connection, engine, and session.
    """

    def __init__(self, host: str, port: int, user: str, password: str,
                 database: str = 'nacsos_core'):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

        # TODO expire_on_commit (check if this should be turned off)
        self._connection_str = f'postgresql+psycopg://{self._user}:{self._password}@' \
                               f'{self._host}:{self._port}/{self._database}'
        self.engine = create_async_engine(self._connection_str, echo=True, future=True,
                                          json_serializer=DictLikeEncoder().encode)
        self._session: async_sessionmaker[AsyncSession] = async_sessionmaker(bind=self.engine, autoflush=False,
                                                                             autocommit=False)

    def startup(self) -> None:
        """
        Call this function to initialise the database engine.
        """
        # SQLModel.metadata.create_all(self.engine)
        pass

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
                 database: str = 'nacsos_core'):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

        # TODO expire_on_commit (check if this should be turned off)
        self._connection_str = f'postgresql+psycopg2://{self._user}:{self._password}@' \
                               f'{self._host}:{self._port}/{self._database}'
        self.engine = create_engine(self._connection_str, echo=True, future=True,
                                    json_serializer=DictLikeEncoder().encode)
        self._session: sessionmaker[Session] = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

    def startup(self) -> None:
        """
        Call this function to initialise the database engine.
        """
        # SQLModel.metadata.create_all(self.engine)
        pass

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
