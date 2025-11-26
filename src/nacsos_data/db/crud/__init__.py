import logging
import uuid
from typing import Any, Type, TypeVar
from sqlalchemy import select
from uuid import UUID, uuid4
from pydantic import BaseModel

from ..engine import ensure_session_async, DBSession
from ..schemas import Base

logger = logging.getLogger('nacsos_data.crud')

T = TypeVar('T')
S = TypeVar('S', bound=Base)
M = TypeVar('M', bound=BaseModel)


@ensure_session_async
async def update_orm(session: DBSession, updated_model: BaseModel, Model: Type[M], Schema: Type[Base], filter_by: dict[str, Any], skip_update: list[str]) -> M:
    stmt = select(Schema).filter_by(**filter_by)
    orm_model = (await session.execute(stmt)).scalars().one_or_none()
    if orm_model is not None:
        for key, value in updated_model.model_dump().items():
            if key in skip_update:
                continue
            setattr(orm_model, key, value)

    await session.flush_or_commit()

    # return the updated project for completeness
    return Model(**orm_model.__dict__)


@ensure_session_async
async def upsert_orm(session: DBSession, upsert_model: BaseModel, Schema: Type[S], primary_key: str, skip_update: list[str] | None = None) -> str | UUID | None:
    # returns id of inserted or updated assignment scope
    logger.debug(f'UPSERT "{Schema}" with keys: {list(upsert_model.model_dump().keys())}')

    p_key: str | UUID | None = getattr(upsert_model, primary_key, None)

    if p_key is None:
        p_key = uuid4()
        setattr(upsert_model, primary_key, p_key)
    else:
        # fetch existing model from the database
        # session.query()
        stmt = select(Schema).filter_by(**{primary_key: p_key})
        orm_model: S | None = (await session.scalars(stmt)).one_or_none()

        logger.debug(f'"{Schema}" with {primary_key}={p_key} found, attempting to UPDATE!')

        # check if it actually already exists in the database
        # if so, update all fields and commit
        if orm_model is not None:
            for key, value in upsert_model.model_dump().items():
                if (skip_update is not None and key in skip_update) or key == primary_key:
                    continue
                # logger.debug(f'{key}: "{value}"')
                setattr(orm_model, key, value)

            await session.flush_or_commit()

            return p_key
        # else: does not exist, create item
        # TODO: would it be a better behaviour to raise an error if key does not exist?

    logger.debug(f'"{Schema}" with {primary_key}={p_key} does not exist yet, attempting to INSERT!')
    orm_model = Schema(**upsert_model.model_dump())
    session.add(orm_model)
    await session.flush_or_commit()

    return p_key


def sentinel_uuid(obj: T, keys: list[str]) -> T:
    for key in keys:
        value = getattr(obj, key) if hasattr(obj, key) else None
        if value is not None:
            setattr(obj, key, uuid.UUID(value))
    return obj
