import os
from pathlib import Path
from urllib.parse import quote

from httpx import BasicAuth
from pydantic_settings import SettingsConfigDict, BaseSettings
from pydantic.networks import PostgresDsn
from pydantic import field_validator, ValidationInfo


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
    def build_connection_string(cls, v: str | None, info: ValidationInfo) -> PostgresDsn:
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

    model_config = SettingsConfigDict(extra='allow')


class OpenAlexConfig(BaseSettings):
    API_KEY: str | None = None

    SOLR_ENDPOINT: str = 'http://localhost:8983'
    SOLR_COLLECTION: str = 'openalex'
    SOLR_USER: str | None = None
    SOLR_PASSWORD: str | None = None

    # Optional info (mainly for meta-cache and local snapshot)
    SNAPSHOT_DIR: str | None = None  # Path to S3 snapshot on disk
    SOLR_BIN: str | None = None  # /path/to/solr/bin directory
    SOLR_HOME: str | None = None  # /path/to/solr-home directory
    SOLR_HOST: str | None = None  # probably same as in SOLR_ENDPOINT
    SOLR_PORT: str | None = None  # probably same as in SOLR_ENDPOINT (probably 8983)
    SOLR_ZOO_PORT: str | None = None  # zookeper port (probably 9983)

    @property
    def solr_url(self) -> str:
        return f'{self.SOLR_ENDPOINT}/solr/{self.SOLR_COLLECTION}'

    @property
    def solr_collections_url(self) -> str:
        return f'{self.SOLR_ENDPOINT}/api/collections/{self.SOLR_COLLECTION}'

    @property
    def auth(self) -> BasicAuth | None:
        if self.SOLR_USER is None or self.SOLR_PASSWORD is None:
            return None
        return BasicAuth(username=self.SOLR_USER, password=self.SOLR_PASSWORD)

    model_config = SettingsConfigDict(extra='allow')


class Settings(BaseSettings):
    DB: DatabaseConfig = DatabaseConfig()
    OPENALEX: OpenAlexConfig = OpenAlexConfig()

    model_config = SettingsConfigDict(case_sensitive=True, env_prefix='NACSOS_', env_nested_delimiter='__', extra='allow')


def load_settings(conf_file: Path | str | None = None) -> Settings:
    if conf_file is None:
        conf_file = os.environ.get('NACSOS_CONFIG', 'config/default.env')

    return Settings(_env_file=conf_file, _env_file_encoding='utf-8')
