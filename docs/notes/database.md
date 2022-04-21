

## PostgreSQL & PGAdmin

To start postgres
```bash
sudo systemctl start docker
cd /path/to/docker-compose-file
docker-compose up
```
PGAdmin available at http://localhost:5050/browser/#  
The first time, you have to configure the server (host: nacsos_postgres, port: 5432)

Some more info:
* https://towardsdatascience.com/how-to-run-postgresql-and-pgadmin-using-docker-3a6a8ae918b5
* https://github.com/lifeparticle/PostgreSql-Snippets/blob/main/pgAdmin/docker-compose.yml


## SQLModel

### Alembic (migration management)
* https://alembic.sqlalchemy.org/en/latest/tutorial.html
* https://github.com/joemudryk/learning-sqlmodel