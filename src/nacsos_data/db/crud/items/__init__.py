from typing import Optional
from sqlalchemy import select, delete, insert, func
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import Item, M2MProjectItem
from nacsos_data.models.items import ItemModel


# TODO paged output with cursor of some sort
#      ideally make it a decorator so it's reusable everywhere
#      https://www.postgresql.org/docs/current/sql-declare.html
async def read_all_items_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) -> list[ItemModel]:
    async with engine.session() as session:
        stmt = select(Item) \
            .join(M2MProjectItem, M2MProjectItem.item_id == Item.item_id) \
            .where(M2MProjectItem.project_id == project_id)
        result = await session.execute(stmt)
        result_list = result.scalars().all()
        return [ItemModel(**res.__dict__) for res in result_list]


async def read_item_count_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) -> int:
    async with engine.session() as session:
        stmt = select(M2MProjectItem.project_id, func.count(M2MProjectItem.item_id).label('num_items')) \
            .where(M2MProjectItem.project_id == project_id) \
            .group_by(M2MProjectItem.project_id)
        result = (await session.execute(stmt)).mappings().one_or_none()
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
