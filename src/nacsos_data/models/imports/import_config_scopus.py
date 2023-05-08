from pydantic import BaseModel


class ImportConfigScopus(BaseModel):
    filenames: list[str]
