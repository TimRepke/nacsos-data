# NACSOS™ nexus – Data Utils
This repository contains the core data model for the NACSOS platform.
Furthermore, it provides core utilities to programmatically access the data.

The main purpose of this package is to be used in the NACSOS backend/frontend infrastructure. 
It can also be imported in any other project, where accessing data through the proper API 
does not provide the required functionality.

## Creating database revision
```bash
nacsos_migrate revision --autogenerate --root-path src/nacsos_data/ --ini-file alembic.ini --message "???"
nacsos_migrate upgrade --revision head --root-path src/nacsos_data/ --ini-file alembic.ini
 
# or directly
# first, uncomment "script_location" line in alembic.ini

# automatically creating up/downgrade script
$ alembic revision --autogenerate -m "Helpful comment"

# apply the migration
$ alembic upgrade head

# revert uncommenting in alembic.ini
```

## Semantic versioning
The general rule of thumb is to
  - increment the third number on every change that is worth deploying into nacsos_core
  - increment the second number on every change that includes a database migration
  - increment the second number on every change that includes deprecations
  - increment the first number when there is a fundamental paradigm shift with loads of deprecations

We count versions starting at zero and always include all three numbers prefixed by 'v'.

## Testing

```bash
# For testing/linting/code style checks, run
flake8 --config .flake8
mypy --config-file=pyproject.toml src/nacsos_data 
```