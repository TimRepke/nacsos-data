from typing import Optional
from sqlalchemy import select, delete
from uuid import UUID
from pydantic import BaseModel

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import Base
from nacsos_data.models import SBaseModel


async def update_orm(updated_model: BaseModel, Model, Schema: Base, filter_by: dict, skip_update: list[str],
                     engine: DatabaseEngineAsync):
    async with engine.session() as session:
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
