from typing import Type
from sqlalchemy import select, func, insert
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import M2MProjectItem, Item, TwitterItem, AnyItemSchema, AnyItemType
from nacsos_data.db.schemas.projects import ProjectType
from nacsos_data.models.items import AnyItemModel, ItemModel, TwitterItemModel, AnyItemModelType
from nacsos_data.models.projects import ProjectTypeLiteral

import logging

logger = logging.getLogger('nacsos-data.crud.items')


async def read_item_count_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) -> int:
    async with engine.session() as session:
        stmt = (select(M2MProjectItem.project_id,
                       func.count(M2MProjectItem.item_id).label('num_items'))
                .where(M2MProjectItem.project_id == project_id)
                .group_by(M2MProjectItem.project_id))
        result = (await session.execute(stmt)).mappings().one_or_none()
        if result is None or 'num_items' not in result or type(result['num_items']) != int:
            return 0
        return result['num_items']


async def create_item(item: ItemModel, project_id: str | UUID | None, engine: DatabaseEngineAsync) -> None:
    try:
        async with engine.session() as session:
            item_id = (await session.execute(insert(Item).values(**item.dict()).returning(Item.item_id))).scalars().one()

            if project_id is not None:
                await session.execute(insert(M2MProjectItem).values(item_id=item_id, project_id=project_id))

    except Exception as e:
        logger.exception(e)


async def create_items(items: list[ItemModel], project_id: str | UUID | None, engine: DatabaseEngineAsync) -> None:
    # TODO make this in an actual batched mode
    for item in items:
        await create_item(item, project_id=project_id, engine=engine)


async def _read_all_for_project(project_id: str | UUID, Schema: Type[AnyItemSchema], Model: Type[AnyItemModelType],
                                engine: DatabaseEngineAsync) -> list[AnyItemModelType]:
    async with engine.session() as session:
        # FIXME We should probably set a LIMIT by default
        stmt = (select(Schema)
                .join(M2MProjectItem, M2MProjectItem.item_id == Schema.item_id)
                .where(M2MProjectItem.project_id == project_id))
        result = (await session.execute(stmt)).scalars().all()
        return [Model(**res.__dict__) for res in result]


async def _read_paged_for_project(project_id: str | UUID, Schema: Type[AnyItemSchema], Model: Type[AnyItemModelType],
                                  page: int, page_size: int, engine: DatabaseEngineAsync) -> list[AnyItemModelType]:
    # page: count starts at 1
    async with engine.session() as session:
        offset = (page - 1) * page_size
        if offset < 0:
            offset = 0
        stmt = (select(Schema)
                .join(M2MProjectItem, M2MProjectItem.item_id == Schema.item_id)
                .where(M2MProjectItem.project_id == project_id)
                .offset(offset)
                .limit(page_size))
        result = (await session.execute(stmt)).scalars().all()
        return [Model(**res.__dict__) for res in result]


def _get_schema_model_for_type(item_type: ProjectTypeLiteral | ProjectType) \
        -> tuple[Type[AnyItemType], Type[AnyItemModel]]:
    if item_type == 'generic' or item_type == ProjectType.generic:
        return Item, ItemModel
    if item_type == 'twitter' or item_type == ProjectType.twitter:
        return TwitterItem, TwitterItemModel
    raise ValueError(f'Not implemented for {item_type}')


async def read_any_item_by_item_id(item_id: str | UUID, item_type: ProjectTypeLiteral | ProjectType,
                                   engine: DatabaseEngineAsync) -> AnyItemModel | None:
    Schema, Model = _get_schema_model_for_type(item_type=item_type)
    async with engine.session() as session:
        stmt = select(Schema).filter_by(item_id=item_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return Model(**result.__dict__)
    return None

# TODO delete item (with cascade to specific item type e.g. tweet, but keep it if its in other projects)
