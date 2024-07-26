import re
import uuid
import logging
from typing import Generator, Callable, TypeAlias, AsyncGenerator, NamedTuple, Literal

from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401

from ...engine import ensure_session_async
from ...schemas import AcademicItem
from ....models.items import AcademicItemModel
from ....util import ensure_logger_async

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


@ensure_session_async
@ensure_logger_async(logger)
async def read_ids_from_db(
        session: AsyncSession,
        log: logging.Logger,
        project_id: str | uuid.UUID,
        field: Literal['item_id', 'doi', 'wos_id', 'scopus_id', 'openalex_id', 's2_id', 'pubmed_id', 'dimensions_id'],
        batch_size: int = 5000,
) -> AsyncGenerator[str, None]:
    if field not in {'item_id', 'doi', 'wos_id', 'scopus_id', 'openalex_id', 's2_id', 'pubmed_id', 'dimensions_id'}:
        raise KeyError(f'Invalid field `{field}`')

    rslt = (await session.stream(text(f'''
    SELECT ai.{field}::text as identifier
    FROM academic_item ai
    WHERE ai.project_id = :project_id
    
    UNION 
    
    SELECT aiv.{field}::text as identifier
    FROM academic_item_variant aiv
    JOIN import ON aiv.import_id = import.import_id
    WHERE import.project_id = :project_id;
    '''), {'project_id': project_id})).mappings().partitions()
    async for batch in rslt:
        log.debug(f'Received batch with {len(batch)} entries.')
        for r in batch:
            yield r['identifier']
