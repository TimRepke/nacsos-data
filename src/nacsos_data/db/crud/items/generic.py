from uuid import UUID
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas.items.generic import GenericItem
from nacsos_data.models.items.generic import GenericItemModel

from . import _read_all_for_project, _read_paged_for_project


async def read_all_generic_items_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) -> list[GenericItemModel]:
    return await _read_all_for_project(project_id=project_id, Schema=GenericItem, Model=GenericItemModel, engine=engine)


async def read_all_generic_items_for_project_paged(project_id: str | UUID, page: int, page_size: int,
                                                 engine: DatabaseEngineAsync) -> list[GenericItemModel]:
    return await _read_paged_for_project(project_id=project_id, page=page, page_size=page_size,
                                         Schema=GenericItem, Model=GenericItemModel, engine=engine)


async def read_generic_item_by_item_id(item_id: str | UUID, engine: DatabaseEngineAsync) -> GenericItemModel | None:
    stmt = select(GenericItem).filter_by(item_id=item_id)
    async with engine.session() as session:  # type: AsyncSession
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return GenericItemModel(**result.__dict__)
    return None
