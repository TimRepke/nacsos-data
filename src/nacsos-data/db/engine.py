from sqlmodel import create_engine, SQLModel
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager


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

        self._connection_str = f'postgresql://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}'

        self.engine = create_engine(self._connection_str, echo=True,
                                    connect_args={'check_same_thread': False})
        self._session = sessionmaker(bind=self.engine)

    def startup(self) -> None:
        """
        Call this function to initialise the database engine.
        """
        SQLModel.metadata.create_all(self.engine)

    @contextmanager
    def session(self):
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
