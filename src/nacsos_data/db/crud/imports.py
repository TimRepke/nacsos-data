from sqlalchemy import select, delete
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.crud import upsert_orm
from nacsos_data.db.schemas import Import, m2m_import_item_table
from nacsos_data.db.schemas.items.base import Item
from nacsos_data.models.imports import ImportModel


async def read_all_imports_for_project(project_id: UUID | str,
                                       engine: DatabaseEngineAsync) -> list[ImportModel]:
    async with engine.session() as session:
        stmt = select(Import).where(Import.project_id == project_id)
        result = (await session.execute(stmt)).scalars().all()
        return [ImportModel.parse_obj(res.__dict__) for res in result]


async def read_item_count_for_import(import_id: UUID | str, engine: DatabaseEngineAsync) -> int:
    async with engine.session() as session:
        stmt = select(m2m_import_item_table).where(m2m_import_item_table.c.import_id == import_id)
        result = len((await session.execute(stmt)).all())
        return result


async def read_import(import_id: UUID | str,
                      engine: DatabaseEngineAsync) -> ImportModel | None:
    async with engine.session() as session:
        stmt = select(Import).where(Import.import_id == import_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return ImportModel.parse_obj(result.__dict__)
        return None


async def upsert_import(import_model: ImportModel,
                        engine: DatabaseEngineAsync) -> str | UUID | None:
    key = await upsert_orm(upsert_model=import_model,
                           Schema=Import,
                           primary_key=Import.import_id.name,
                           db_engine=engine)
    return key


async def delete_import(import_id: UUID | str,
                        engine: DatabaseEngineAsync) -> None:
    """
    When an import is deleted, we want to also delete all items that belonged to that import
    and that import only.
    """
    async with engine.session() as session:
        # Delete import
        stmt = delete(Import).where(Import.import_id == import_id)
        await session.execute(stmt)

        # Delete items that no longer belong to any imports (this will cascade to delete their
        # assignments and annotations, so be careful!
        stmt = delete(Item).where(~Item.imports.any())
        await session.execute(stmt)
