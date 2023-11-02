import logging
from typing import Type
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import (
    Item,
    TwitterItem,
    GenericItem,
    AcademicItem,
    ItemType,
    ItemTypeLiteral,
    AnyItemType,
    AnyItemSchema,
    LexisNexisItem,
    LexisNexisItemSource
)
from nacsos_data.models.items import (
    GenericItemModel,
    TwitterItemModel,
    AcademicItemModel,
    AnyItemModelType,
    AnyItemModel,
    LexisNexisItemModel,
    LexisNexisItemSourceModel,
    FullLexisNexisItemModel
)

from .query import query_to_sql, Query

logger = logging.getLogger('nacsos-data.crud.items')


async def read_item_count_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) -> int:
    session: AsyncSession
    async with engine.session() as session:
        result: int = (await session.execute(  # type: ignore[assignment] # FIXME: mypy
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
        return [Model.model_validate(res.__dict__) for res in result]


async def read_paged_for_project(project_id: str | UUID, Schema: Type[AnyItemSchema], Model: Type[AnyItemModelType],
                                 page: int, page_size: int, engine: DatabaseEngineAsync) -> list[AnyItemModelType]:
    # page: count starts at 1
    session: AsyncSession
    async with engine.session() as session:
        offset = (page - 1) * page_size
        if offset < 0:
            offset = 0
        stmt = (select(Schema)
                .where(Schema.project_id == project_id)
                .offset(offset)
                .limit(page_size))
        result = (await session.execute(stmt)).scalars().all()

        ret = []
        for res in result:
            try:
                ret.append(Model(**res.__dict__))
            except Exception as e:
                logger.error(res)
                logger.debug(res.__dict__)
                raise e
        return ret


def _get_schema_model_for_type(item_type: ItemType | ItemTypeLiteral) \
        -> tuple[Type[AnyItemType], Type[AnyItemModel]]:
    if item_type == 'generic' or item_type == ItemType.generic:
        return GenericItem, GenericItemModel
    if item_type == 'twitter' or item_type == ItemType.twitter:
        return TwitterItem, TwitterItemModel
    if item_type == 'academic' or item_type == ItemType.academic:
        return AcademicItem, AcademicItemModel
    if item_type == 'lexis' or item_type == ItemType.lexis:
        return LexisNexisItem, LexisNexisItemModel

    raise ValueError(f'Not implemented for {item_type}')


async def read_any_item_by_item_id(item_id: str | UUID, item_type: ItemType | ItemTypeLiteral,
                                   engine: DatabaseEngineAsync) -> AnyItemModel | None:
    if item_type == ItemType.lexis or item_type == 'lexis':
        async with engine.session() as session:
            lexis_stmt = (
                select(LexisNexisItem,
                       func.array_agg(
                           func.row_to_json(
                               LexisNexisItemSource.__table__.table_valued()  # type: ignore[attr-defined]
                           )
                       ).label('sources'))
                .join(LexisNexisItemSource, LexisNexisItemSource.item_id == LexisNexisItem.item_id)
                .where(LexisNexisItem.item_id == item_id)
                .group_by(LexisNexisItem.item_id, Item.item_id)
            )
            rslt = (await session.execute(lexis_stmt)).mappings().one_or_none()
            if rslt is not None:
                sources = [LexisNexisItemSourceModel.model_validate(src) for src in rslt['sources']]
                item = FullLexisNexisItemModel(**rslt['LexisNexisItem'].__dict__)
                item.sources = sources
                return item
    else:
        Schema, Model = _get_schema_model_for_type(item_type=item_type)
        async with engine.session() as session:
            stmt = select(Schema).filter_by(item_id=item_id)
            result = (await session.execute(stmt)).scalars().one_or_none()
            if result is not None:
                return Model.model_validate(result.__dict__)
    return None


__all__ = ['Query', 'query_to_sql',
           'read_all_for_project', 'read_paged_for_project',
           'read_item_count_for_project', 'read_any_item_by_item_id']
