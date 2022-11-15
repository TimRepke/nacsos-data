from typing import Type
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import Item, TwitterItem, AnyItemSchema, AnyItemType, ItemType, ItemTypeLiteral, GenericItem
from nacsos_data.models.items import AnyItemModel, TwitterItemModel, AnyItemModelType, GenericItemModel

import logging

logger = logging.getLogger('nacsos-data.crud.items')


async def read_item_count_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) -> int:
    async with engine.session() as session:  # type: AsyncSession
        result: int = (await session.execute(
            select(func.count(Item.item_id)).where(Item.project_id == project_id)
        )).scalar()
        return result


async def read_all_for_project(project_id: str | UUID, Schema: Type[AnyItemSchema], Model: Type[AnyItemModelType],
                               engine: DatabaseEngineAsync) -> list[AnyItemModelType]:
    async with engine.session() as session:
        # FIXME We should probably set a LIMIT by default
        stmt = (select(Schema)
                .where(Schema.project_id == project_id))
        result = (await session.execute(stmt)).scalars().all()
        return [Model.parse_obj(res.__dict__) for res in result]


async def read_paged_for_project(project_id: str | UUID, Schema: Type[AnyItemSchema], Model: Type[AnyItemModelType],
                                 page: int, page_size: int, engine: DatabaseEngineAsync) -> list[AnyItemModelType]:
    # page: count starts at 1
    async with engine.session() as session:
        offset = (page - 1) * page_size
        if offset < 0:
            offset = 0
        stmt = (select(Schema)
                .where(Schema.project_id == project_id)
                .offset(offset)
                .limit(page_size))
        result = (await session.execute(stmt)).scalars().all()
        return [Model(**res.__dict__) for res in result]


def _get_schema_model_for_type(item_type: ItemType | ItemTypeLiteral) \
        -> tuple[Type[AnyItemType], Type[AnyItemModel]]:
    if item_type == 'generic' or item_type == ItemType.generic:
        return GenericItem, GenericItemModel
    if item_type == 'twitter' or item_type == ItemType.twitter:
        return TwitterItem, TwitterItemModel
    # FIXME: add academic
    raise ValueError(f'Not implemented for {item_type}')


async def read_any_item_by_item_id(item_id: str | UUID, item_type: ItemType | ItemTypeLiteral,
                                   engine: DatabaseEngineAsync) -> AnyItemModel | None:
    Schema, Model = _get_schema_model_for_type(item_type=item_type)
    async with engine.session() as session:
        stmt = select(Schema).filter_by(item_id=item_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return Model.parse_obj(result.__dict__)
    return None
