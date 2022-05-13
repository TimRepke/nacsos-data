# NACSOS™ nexus – Data Utils
This repository contains the core data model for the NACSOS platform.
Furthermore, it provides core utilities to programmatically access the data.

The main purpose of this package is to be used in the NACSOS backend/frontend infrastructure. 
It can also be imported in any other project, where accessing data through the proper API 
does not provide the required functionality.

## Creating database revision
```bash
# automatically creating up/downgrade script
$ alembic revision --autogenerate -m "Helpful comment"

# apply the migration
$ alembic upgrade head
```