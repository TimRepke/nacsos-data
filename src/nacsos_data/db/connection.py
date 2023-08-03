import os
from urllib.parse import quote

from pydantic_settings import SettingsConfigDict, BaseSettings
from pydantic.networks import PostgresDsn
from pydantic import field_validator, FieldValidationInfo

from .engine import DatabaseEngine, DatabaseEngineAsync


class DatabaseConfig(BaseSettings):
    SCHEME: str = 'postgresql'
    SCHEMA: str = 'public'
    HOST: str = 'localhost'  # host of the db server
    PORT: int = 5432  # port of the db server
    USER: str = 'nacsos'  # username for the database
    PASSWORD: str = 'secr€t_passvvord'  # password for the database user
    DATABASE: str = 'nacsos_core'  # name of the database

    CONNECTION_STR: PostgresDsn | None = None

    @field_validator('CONNECTION_STR', mode='before')
    def build_connection_string(cls, v: str | None, info: FieldValidationInfo) -> PostgresDsn:
        assert info.config is not None

        if isinstance(v, str):
            raise ValueError('This field will be generated automatically, please do not use it.')

        return PostgresDsn.build(
            scheme=info.data.get('SCHEME', 'postgresql'),
            username=info.data.get('USER'),
            password=quote(info.data.get('PASSWORD')),  # type: ignore[arg-type]
            host=info.data.get('HOST'),
            port=info.data.get('PORT'),
            path=f'{info.data.get("DATABASE", "")}',
        )

    model_config = SettingsConfigDict(env_prefix='NACSOS_DB__')


def _get_settings(conf_file: str | None = None) -> DatabaseConfig:
    if conf_file is None:
        conf_file = os.environ.get('NACSOS_CONFIG', 'config/default.env')

    # FIXME: get rid of ignore here
    return DatabaseConfig(_env_file=conf_file, _env_file_encoding='utf-8')  # type: ignore[call-arg]


def get_engine(conf_file: str | None = None,
               settings: DatabaseConfig | None = None,
               debug: bool = False) -> DatabaseEngine:
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
    if settings is None:
        settings = _get_settings(conf_file)

    return DatabaseEngine(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD,
                          database=settings.DATABASE, debug=debug)


def get_engine_async(conf_file: str | None = None,
                     debug: bool = False) -> DatabaseEngineAsync:
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
                               database=settings.DATABASE, debug=debug)
