from .engine import DatabaseEngine, DatabaseEngineAsync
from ..util.conf import DatabaseConfig, load_settings


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
        if conf_file is None:
            raise AssertionError('Neither `settings` not `conf_file` specified.')
        settings = load_settings(conf_file).DB

    return DatabaseEngine(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD,
                          database=settings.DATABASE, debug=debug)


def get_engine_async(conf_file: str | None = None,
                     settings: DatabaseConfig | None = None,
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
    if settings is None:
        if conf_file is None:
            raise AssertionError('Neither `settings` not `conf_file` specified.')
        settings = load_settings(conf_file).DB

    return DatabaseEngineAsync(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD,
                               database=settings.DATABASE, debug=debug)
