from pydantic import BaseModel


class SBaseModel(BaseModel):
    """Allows a BaseModel to return its fields by string variable indexing"""

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, key, value):
        setattr(self, key, value)

