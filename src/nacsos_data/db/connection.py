import os
from typing import Any

from pydantic import BaseSettings, PostgresDsn, validator

from .engine import DatabaseEngine, DatabaseEngineAsync


class DatabaseConfig(BaseSettings):
    HOST: str = 'localhost'  # host of the db server
    PORT: int = 5432  # port of the db server
    USER: str = 'nacsos'  # username for the database
    PASSWORD: str = 'secr€t_passvvord'  # password for the database user
    DATABASE: str = 'nacsos_core'  # name of the database

    CONNECTION_STR: PostgresDsn | None = None

    @validator('CONNECTION_STR', pre=True)
    def build_connection_string(cls, v: str | None, values: dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return PostgresDsn.build(
            scheme="postgresql",
            user=values.get('USER'),
            password=values.get('PASSWORD'),
            host=values.get('HOST'),
            path=f'/{values.get("DATABASE", "")}',
        )

    class Config:
        # Add this prefix to be compatible with nacsos-core config
        env_prefix = 'NACSOS_DB__'


def _get_settings(conf_file: str | None = None) -> DatabaseConfig:
    if conf_file is None:
        conf_file = os.environ.get('NACSOS_CONFIG', 'config/default.env')

    # FIXME: get rid of ignore here
    return DatabaseConfig(_env_file=conf_file, _env_file_encoding='utf-8')  # type: ignore[call-arg]


def get_engine(conf_file: str | None = None) -> DatabaseEngine:
    """
    Returns a database connection (aka DatabaseEngine).

    Ways to inject connection settings:
     - via environment variables (e.g. `NACSOS_DB__HOST` to set the host)
     - via providing path to .env file that contains the settings (env vars)
     - letting it use the fallback path (but try to see if env var `NACSOS_CONFIG` is that that points to the settings

    See DatabaseConfig for default values.
    Config file overrides default values (if variable is provided).
    Environment variables overrides values in config (if variable is provided).

    For more details on configuration, see: https://pydantic-docs.helpmanual.io/usage/settings/

    Example usage:
    ```
    from nacsos_data.db import get_engine
    db_engine = get_engine('path/to/config.env')

    with db_engine.session() as session:
        # generate a query
        stmt = select(TwitterItem).filter_by(twitter_author_id=twitter_author_id)
        # execute the query
        result = session.execute(stmt)
        # fetch data and translate to dictionaries (optional)
        result_list = result.scalars().all()
        # transform to pydantic models (optional)
        result_tweets = [TwitterItemModel(**res.__dict__) for res in result_list]
    ```
    Fore more details on how to query data with sqlalchemy, see https://docs.sqlalchemy.org/en/20/orm/quickstart.html#simple-select
    """
    settings = _get_settings(conf_file)
    return DatabaseEngine(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD,
                          database=settings.DATABASE)


def get_engine_async(conf_file: str | None = None) -> DatabaseEngineAsync:
    """
    Same as `get_engine()`, but returns async db engine.

    NOTE: Remember that session executions need to be awaited!
    ```
        # Example above
        result = session.execute(stmt)
        # ... needs to be
        result = await session.execute(stmt)
    ```
    """
    settings = _get_settings(conf_file)
    return DatabaseEngineAsync(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD,
                               database=settings.DATABASE)
