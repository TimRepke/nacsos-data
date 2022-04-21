from typing import Optional

from sqlmodel import SQLModel, Field
from sqlalchemy import Column, TEXT


class BaseItem(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    text: str = Field(sa_column=Column(TEXT))
