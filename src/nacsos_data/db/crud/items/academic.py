import re
import uuid
import logging
from typing import Generator, Callable, TypeAlias, AsyncGenerator, NamedTuple

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401

from ...schemas import AcademicItem
from ....models.items import AcademicItemModel

logger = logging.getLogger('nacsos_data.crud.items.academic')

AcademicItemGenerator: TypeAlias = Callable[[], Generator[AcademicItemModel, None, None]]


class ItemEntry(NamedTuple):
    item_id: str
    text: str


REG_CLEAN = re.compile(r'[^a-z ]+', flags=re.IGNORECASE)


def itm2txt(r: object) -> str:
    # Abstract *has* to exist, otherwise do not continue
    if getattr(r, 'text') is None:
        return ''
    return REG_CLEAN.sub(' ', f'{getattr(r, "title", "") or ""} {getattr(r, "text", "") or ""}'.lower()).strip()


def gen_academic_entries(it: Generator[AcademicItemModel, None, None]) -> Generator[ItemEntry, None, None]:
    for itm in it:
        yield ItemEntry(item_id=str(itm.item_id), text=itm2txt(itm))


async def read_item_entries_from_db(session: AsyncSession,
                                    project_id: str | uuid.UUID,
                                    log: logging.Logger | None = None,
                                    batch_size: int = 500,
                                    min_text_len: int = 250) -> AsyncGenerator[list[ItemEntry], None]:
    if log is None:
        log = logger
    stmt = (select(AcademicItem.item_id, AcademicItem.title, AcademicItem.text)
            .where(AcademicItem.project_id == project_id,
                   AcademicItem.text.isnot(None),
                   func.char_length(AcademicItem.text) > min_text_len)
            .execution_options(yield_per=batch_size))
    rslt = (await session.stream(stmt)).mappings().partitions()

    async for batch in rslt:
        log.debug(f'Received batch with {len(batch)} entries.')
        prepared = [(str(r['item_id']), itm2txt(r)) for r in batch]

        yield [
            ItemEntry(item_id=item_id, text=text)
            for item_id, text in prepared
            if text and len(text) > min_text_len
        ]
