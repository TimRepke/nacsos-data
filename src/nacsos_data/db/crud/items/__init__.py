from typing import Sequence
from sqlalchemy import select, insert, func, RowMapping
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import M2MProjectItem, AnyItemType, Item, TwitterItem
from nacsos_data.db.schemas.projects import ProjectType
from nacsos_data.models.items import AnyItemModel, ItemModel, TwitterItemModel
from nacsos_data.models.projects import ProjectTypeLiteral


async def read_item_count_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) -> int:
    async with engine.session() as session:
        stmt = select(M2MProjectItem.project_id, func.count(M2MProjectItem.item_id).label('num_items')) \
            .where(M2MProjectItem.project_id == project_id) \
            .group_by(M2MProjectItem.project_id)
        result = (await session.execute(stmt)).mappings().one_or_none()
        if result is None or 'num_items' not in result:
            return 0
        return result['num_items']


async def create_item(item: ItemModel, project_id: str | UUID | None, engine: DatabaseEngineAsync):
    try:
        async with engine.session() as session:
            orm_item = Item(**item.dict())
            session.add(orm_item)

            await session.commit()

            if project_id is not None:
                stmt = insert(M2MProjectItem(item_id=orm_item.item_id, project_id=project_id))
                await session.execute(stmt)

    except Exception as e:
        print(e)


async def create_items(items: list[ItemModel], project_id: str | UUID | None, engine: DatabaseEngineAsync):
    # TODO make this in an actual batched mode
    for item in items:
        await create_item(item, project_id=project_id, engine=engine)


async def _read_all_for_project(project_id: str | UUID, Schema: AnyItemType, Model: AnyItemModel,
                                engine: DatabaseEngineAsync) -> list[AnyItemModel]:
    async with engine.session() as session:
        # FIXME We should probably set a LIMIT by default
        stmt = select(Schema) \
            .join(M2MProjectItem, M2MProjectItem.item_id == Schema.item_id) \
            .where(M2MProjectItem.project_id == project_id)
        result = (await session.execute(stmt)).scalars().all()
        return [Model(**res.__dict__) for res in result]


async def _read_paged_for_project(project_id: str | UUID, Schema: AnyItemType, Model: AnyItemModel,
                                  page: int, page_size: int, engine: DatabaseEngineAsync) -> list[AnyItemModel]:
    # page: count starts at 1
    async with engine.session() as session:
        offset = (page - 1) * page_size
        if offset < 0:
            offset = 0
        stmt = select(Schema) \
            .join(M2MProjectItem, M2MProjectItem.item_id == Schema.item_id) \
            .where(M2MProjectItem.project_id == project_id) \
            .offset(offset) \
            .limit(page_size)
        result = (await session.execute(stmt)).scalars().all()
        return [Model(**res.__dict__) for res in result]


def _get_schema_model_for_type(item_type: ProjectTypeLiteral | ProjectType) -> tuple[AnyItemType, AnyItemModel]:
    if item_type == 'basic' or item_type == ProjectType.basic:
        return Item, ItemModel
    if item_type == 'twitter' or item_type == ProjectType.twitter:
        return TwitterItem, TwitterItemModel
    raise ValueError(f'Not implemented for {item_type}')


async def read_any_item_by_item_id(item_id: str | UUID, item_type: ProjectTypeLiteral,
                                   engine: DatabaseEngineAsync) -> AnyItemModel:
    Schema, Model = _get_schema_model_for_type(item_type=item_type)
    stmt = select(Schema).filter_by(item_id=item_id)
    async with engine.session() as session:
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return Model(**result.__dict__)

# TODO delete item (with cascade to specific item type e.g. tweet, but keep it if its in other projects)
