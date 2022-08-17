import logging
from typing import Any, Type
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID, uuid4
from pydantic import BaseModel

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import Base
from nacsos_data.models import SBaseModel

logger = logging.getLogger('nacsos_data.crud')


class DuplicateKeyWarning(UserWarning):
    """
    This warning is raised when a user tries to insert
    something with a unique key that already exists.
    It's not considered an Exception, since trying to
    insert is a valid process for duplicate-free insertion.
    """
    pass


async def update_orm(updated_model: BaseModel, Model: Type[BaseModel], Schema: Type[Base],
                     filter_by: dict[str, Any], skip_update: list[str], engine: DatabaseEngineAsync) -> Base:
    async with engine.session() as session:  # type: AsyncSession
        stmt = select(Schema).filter_by(**filter_by)
        orm_model = (await session.execute(stmt)).one_or_none()
        if orm_model is not None:
            for key, value in updated_model.dict().items():
                if key in skip_update:
                    continue
                orm_model[key] = value
        await session.commit()

        # return the updated project for completeness
        return Model(**orm_model.__dict__)


async def upsert_orm(upsert_model: SBaseModel, Schema: Type[Base], primary_key: str,
                     engine: DatabaseEngineAsync, skip_update: list[str] = None) -> str | UUID | None:
    # returns id of inserted or updated assignment scope
    async with engine.session() as session:  # type: AsyncSession
        logger.debug(f'UPSERT "{Schema}" with keys: {list(upsert_model.dict().keys())}')

        if upsert_model[primary_key] is None:
            upsert_model[primary_key] = uuid4()
        else:
            # fetch existing model from the database
            # session.query()
            stmt = select(Schema).filter_by(**{primary_key: upsert_model[primary_key]})
            orm_model: Base = (await session.scalars(stmt)).one_or_none()

            logger.debug(f'"{Schema}" with {primary_key}={upsert_model[primary_key]} found, '
                         f'attempting to UPDATE!')

            # check if it actually already exists in the database
            # if so, update all fields and commit
            if orm_model is not None:
                for key, value in upsert_model.dict().items():
                    if (skip_update is not None and key in skip_update) or key == primary_key:
                        continue
                    # logger.debug(f'{key}: "{value}"')
                    setattr(orm_model, key, value)

                await session.commit()

                return upsert_model[primary_key]
            # else: does not exist, create item
            # TODO: would it be a better behaviour to raise an error if key does not exist?

        logger.debug(f'"{Schema}" with {primary_key}={upsert_model[primary_key]} does not exist yet, '
                     f'attempting to INSERT!')
        orm_model = Schema(**upsert_model.dict())
        session.add(orm_model)
        await session.commit()

        return upsert_model[primary_key]
