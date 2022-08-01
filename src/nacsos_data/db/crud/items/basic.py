from uuid import UUID
from sqlalchemy import select

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import Item
from nacsos_data.models.items import ItemModel

from . import _read_all_for_project, _read_paged_for_project


async def read_all_basic_items_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) -> list[ItemModel]:
    return await _read_all_for_project(project_id=project_id, Schema=Item, Model=ItemModel, engine=engine)


async def read_all_basic_items_for_project_paged(project_id: str | UUID, page: int, page_size: int,
                                                 engine: DatabaseEngineAsync) -> list[ItemModel]:
    return await _read_paged_for_project(project_id=project_id, page=page, page_size=page_size,
                                         Schema=Item, Model=ItemModel, engine=engine)


async def read_basic_item_by_item_id(item_id: str | UUID, engine: DatabaseEngineAsync) -> ItemModel:
    stmt = select(Item).filter_by(item_id=item_id)
    async with engine.session() as session:
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return ItemModel(**result.__dict__)
