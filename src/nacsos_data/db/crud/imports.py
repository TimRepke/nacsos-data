from sqlalchemy import select, func
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from . import upsert_orm
from nacsos_data.db.schemas import Import, M2MImportItem
from nacsos_data.models.imports import ImportModel


async def read_all_imports_for_project(project_id: UUID | str,
                                       engine: DatabaseEngineAsync) -> list[ImportModel]:
    async with engine.session() as session:
        stmt = select(Import).where(Import.project_id == project_id)
        result = (await session.execute(stmt)).scalars().all()
        return [ImportModel.parse_obj(res.__dict__) for res in result]


async def read_item_count_for_import(import_id: UUID | str, engine: DatabaseEngineAsync) -> int:
    async with engine.session() as session:
        stmt = select(M2MImportItem.import_id, func.count(M2MImportItem.item_id).label('num_items')) \
            .where(M2MImportItem.import_id == import_id) \
            .group_by(M2MImportItem.import_id)
        result = (await session.execute(stmt)).mappings().one_or_none()
        if result is None:
            return 0
        return result['num_items']


async def read_import(import_id: UUID | str,
                      engine: DatabaseEngineAsync) -> ImportModel:
    async with engine.session() as session:
        stmt = select(Import).where(Import.import_id == import_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        return ImportModel.parse_obj(result.__dict__)


async def upsert_import(import_model: ImportModel,
                        engine: DatabaseEngineAsync) -> str | UUID | None:
    print(import_model.config)
    key = await upsert_orm(upsert_model=import_model,
                           Schema=Import,
                           primary_key=Import.import_id.name,
                           engine=engine)
    return key
