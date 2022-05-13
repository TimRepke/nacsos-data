from typing import Optional
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from . import models


class DatabaseEngine:
    """
    This class is the main entry point to access the database.
    It handles the connection, engine, and session.
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str = 'nacsos_core'):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

        self._connection_str = f'postgresql+psycopg://{self._user}:{self._password}@' \
                               f'{self._host}:{self._port}/{self._database}'
        self.engine = create_async_engine(self._connection_str, echo=True, future=True)

        self._session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

    # def startup(self) -> None:
    #     """
    #     Call this function to initialise the database engine.
    #     """
    #     SQLModel.metadata.create_all(self.engine)

    @contextmanager
    def session(self) -> Optional[Session]:
        """

        :return:
        """
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
