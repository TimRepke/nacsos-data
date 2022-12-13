from pydantic import BaseModel


class ImportConfigWoS(BaseModel):
    filenames: list[str]
