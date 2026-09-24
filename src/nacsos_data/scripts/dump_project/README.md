# Export a project

```bash
./src/nacsos_data/scripts/dump_project/export.sh -i  ce9a954f-a3c3-4784-b732-19f3fda31e2e -H localhost -p 5000 -u nacsos_user -o .exports
```

If you don't want to be asked for a password, you can create a pgpassfile with multiple configs, per line:

```
hostname:port:database:username:password
```

In that case, pass the file path if it's not the default:

```bash
./src/nacsos_data/scripts/dump_project/export.sh -i  ce9a954f-a3c3-4784-b732-19f3fda31e2e -H localhost -p 5000 -u nacsos_user -o .exports -c config/.pgpass
```

# Import a project

This is assuming you have a local postgres instance running, and you have a user with the permission to create a database (and become the
owner).
This will then create a database, create the empty schemata from the export and import all (partial) tables.

By default, the password for all users on the platform will be "demo".

```bash
 ./src/nacsos_data/scripts/dump_project/import.sh -o .exports/20260924_ce9a954f-a3c3-4784-b732-19f3fda31e2e -H localhost -p 5432 -u root -c config/.pgpass
```