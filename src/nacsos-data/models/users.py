from typing import Optional

from sqlmodel import SQLModel, Field


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, nullable=False)
    full_name: str
    email: str = Field(index=True, nullable=False)
    password: str = Field(nullable=False)
    is_superuser: bool = Field(default=False)
    is_active: bool = Field(default=True)
