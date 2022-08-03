from typing import Any
from pydantic import BaseModel


class SBaseModel(BaseModel):
    """Allows a BaseModel to return its fields by string variable indexing"""

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)
