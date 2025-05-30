import logging
from typing import Sequence

from sqlalchemy import select, func, RowMapping
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID

from nacsos_data.db.schemas import (
    Item,
    LexisNexisItem, LexisNexisItemSource
)
from nacsos_data.models.items import (
    LexisNexisItemSourceModel,
    FullLexisNexisItemModel
)

from ...engine import ensure_session_async

logger = logging.getLogger('nacsos-data.crud.items')


@ensure_session_async
async def read_lexis_paged_for_project(session: AsyncSession,
                                       project_id: str | UUID,
                                       page: int,
                                       page_size: int) -> list[FullLexisNexisItemModel]:
    offset = (page - 1) * page_size
    if offset < 0:
        offset = 0
    stmt = (select(LexisNexisItem,
                   func.array_agg(
                       func.row_to_json(
                           LexisNexisItemSource.__table__.table_valued()  # type: ignore[attr-defined]
                       )
                   ).label('sources_grp'))
            .join(LexisNexisItemSource, LexisNexisItemSource.item_id == LexisNexisItem.item_id)
            .where(LexisNexisItem.project_id == project_id)
            .group_by(LexisNexisItem.item_id, Item.item_id)
            .limit(page_size)
            .offset(offset))
    rslt = (await session.execute(stmt)).mappings().all()
    return lexis_orm_to_model(rslt)


def lexis_orm_to_model(rslt: Sequence[RowMapping]) -> list[FullLexisNexisItemModel]:
    ret = []
    for res in rslt:
        try:
            sources = [LexisNexisItemSourceModel.model_validate(src) for src in getattr(res, 'sources_grp')]
            item = FullLexisNexisItemModel.model_validate(res['LexisNexisItem'].__dict__)
            item.sources = sources
            ret.append(item)
        except Exception as e:
            logger.error(res)
            logger.debug(res.__dict__)
            raise e
    return ret
