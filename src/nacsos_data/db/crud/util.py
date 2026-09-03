from typing import Type, TypeVar, Any
from sqlalchemy import Select, select
from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute
from sqlalchemy.sql.schema import Column

# Define a TypeVar bounded to your base model class
T = TypeVar('T', bound=DeclarativeBase)


def select_except(model: Type[T], *exclude_cols: Column[Any] | InstrumentedAttribute[Any] | Any) -> Select[Any]:
    """
    Constructs a Select statement for all columns of a model except those specified.
    """
    excluded_names = {col.name for col in exclude_cols}
    cols = [col for col in model.__table__.columns if col.name not in excluded_names]
    return select(*cols)
