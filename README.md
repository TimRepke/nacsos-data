# NACSOS™ nexus – Data Utils
[![Volkswagen status](.ci/volkswargen_ci.svg)](https://github.com/auchenberg/volkswagen)

This repository contains the core data model for the NACSOS platform.
Furthermore, it provides core utilities to programmatically access the data.

The main purpose of this package is to be used in the NACSOS backend/frontend infrastructure. 
It can also be imported in any other project, where accessing data through the proper API 
does not provide the required functionality.

## Milvus via docker
```
cd /srv/milvus
sudo milvus/scripts/standalone_embed.sh start
```

## Setup for development
```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install uv
uv pip install -e ".[testing,utils,priority,scripts]"
```
You can leave out the extras that you don't need.
The minimal installation for development is probably `pip install -e ".[utils]"` but it mostly also works with no extras!

## Creating database revision
```bash
nacsos_migrate revision --autogenerate --root-path src/nacsos_data/ --ini-file alembic.ini --message "???"
nacsos_migrate upgrade --revision head --root-path src/nacsos_data/ --ini-file alembic.ini
# or
nacsos_migrate upgrade --revision head --root-path src/nacsos_data/ --ini-file alembic.secret.ini
 
# or directly
# first, uncomment "script_location" line in alembic.ini

# automatically creating up/downgrade script
$ alembic revision --autogenerate -m "Helpful comment"

# apply the migration
$ alembic upgrade head

# revert uncommenting in alembic.ini
```

```bash
# Running without installing
$ export PYTHONPATH=/home/user/workspace/nacsos_data/src
$ python -m src.nacsos_data.scripts.migrations:run revision --autogenerate --root-path src/nacsos_data/ --ini-file alembic.ini --message "drop times"
```

## Semantic versioning
The general rule of thumb is to
  - increment the third number on every change that is worth deploying into nacsos_core
  - increment the second number on every change that includes a database migration
  - increment the second number on every change that includes deprecations
  - increment the first number when there is a fundamental paradigm shift with loads of deprecations

We count versions starting at zero and always include all three numbers prefixed by 'v'.

The current version is defined in `setup.py` and the respective commit is tagged via git.
This can be done either via `git tag ...` or [PIK GitLab](https://gitlab.pik-potsdam.de/mcc-apsis/nacsos/nacsos-data/-/tags)

## Create staging copy of the database
```bash

# Dump database
# See https://www.postgresql.org/docs/current/app-pgdump.html
pg_dump -C -Fd -Z 9 -j 12 -p 5432 -f pgstash -d nacsos_core -h 0.0.0.0

# Spin up staging cluster
sudo pg_createcluster 15 nacsos_staging --start

sudo vim /etc/postgresql/15/nacsos_staging/pg_hba.conf
# add following line:
# host    all             all              10.10.13.45/0           scram-sha-256
sudo vim /etc/postgresql/15/nacsos_staging/postgresql.conf
# add following line:
# listen_addresses = '0.0.0.0'
# change:
# port = 8010  
sudo pg_ctlcluster 15 nacsos_staging restart

pg_lsclusters

# Import database (adjust port if necessary)
sudo mv pgstash /var/lib/postgresql
sudo chmod -R ug+x /var/lib/postgresql/pgstash
sudo chown -R postgres:postgres /var/lib/postgresql/pgstash
sudo -u postgres bash
cd /var/lib/postgresql
createdb -p 8010 -U postgres nacsos_core
createuser -p 8010 -s nacsos -P
createuser -p 8010 nacsos_read
createuser -p 8010 nacsos_user
/usr/lib/postgresql/16/bin/pg_restore -F d -j 12 -p 8010 -C -d nacsos_core pgstash/

# Remove dump, not needed anymore
rm -r pgstash

# If you don't need it anymore, free up space via
sudo pg_dropcluster 15 nacsos_staging --stop
```

## Testing

```bash
# For testing/linting/code style checks, run
flake8 --config .flake8
mypy --config-file=pyproject.toml src/nacsos_data 
```
