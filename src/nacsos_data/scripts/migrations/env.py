import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool, URL

from alembic import context

from nacsos_data.db.schemas import Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata


# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_url(fallback=True):
    env = [os.getenv('NACSOS_DB__USER'),
           os.getenv('NACSOS_DB__PASSWORD'),
           os.getenv('NACSOS_DB__HOST'),
           os.getenv('NACSOS_DB__PORT'),
           os.getenv('NACSOS_DB__DATABASE')]

    if all(env):
        url = URL.create(drivername='postgresql+psycopg',
                          username=env[0],
                          password=env[1],
                          host=env[2],
                          port=env[3],
                          database=env[4])
        print(f'Using URL from env vars: {url}')
        return url
    if fallback:
        url = config.get_main_option("sqlalchemy.url")
        print(f'Using URL from config: {url}')
        return url
    print('Returning without fallback for URL.')


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    print('Running offline migrations.')

    url = get_url()

    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    print('Running online migrations.')
    conf_section = config.get_section(config.config_ini_section)

    url = get_url(fallback=False)
    if url is not None:
        conf_section['sqlalchemy.url'] = url

    print(conf_section)

    connectable = engine_from_config(
        conf_section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
