import re
import uuid
import logging
from collections import defaultdict
from typing import Generator, Callable, TypeAlias, AsyncGenerator, Literal

from sqlalchemy import select, func, text, cast, TEXT
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401

from ...schemas import AcademicItem, m2m_import_item_table
from ....models.items import AcademicItemModel, ItemEntry
from ....util import gather_async
from ....util.text import itm2txt

logger = logging.getLogger('nacsos_data.crud.items.academic')

AcademicItemGenerator: TypeAlias = Callable[[], Generator[AcademicItemModel, None, None]]

REG_CLEAN = re.compile(r'[^a-z ]+', flags=re.IGNORECASE)


def gen_academic_entries(it: Generator[AcademicItemModel, None, None]) -> Generator[ItemEntry, None, None]:
    for itm in it:
        if itm2txt(itm) != '':
            # Don't bother trying to deduplicate items with no text
            yield ItemEntry(item_id=str(itm.item_id), text=itm2txt(itm))


async def read_item_entries_from_db(
    session: AsyncSession, project_id: str | uuid.UUID, log: logging.Logger | None = None, batch_size: int = 500, min_text_len: int = 250
) -> AsyncGenerator[list[ItemEntry], None]:
    if log is None:
        log = logger
    stmt = (
        select(AcademicItem.item_id, AcademicItem.title, AcademicItem.text)
        .where(AcademicItem.project_id == project_id, AcademicItem.text.isnot(None), func.char_length(AcademicItem.text) > min_text_len)
        .execution_options(yield_per=batch_size)
    )
    rslt = (await session.stream(stmt)).mappings().partitions()

    async for batch in rslt:
        log.debug(f'Received batch with {len(batch)} entries.')
        prepared = [(str(r['item_id']), itm2txt(r)) for r in batch]

        yield [ItemEntry(item_id=item_id, text=text) for item_id, text in prepared if text and len(text) > min_text_len]


IdField = Literal['item_id', 'doi', 'wos_id', 'scopus_id', 'openalex_id', 's2_id', 'pubmed_id', 'dimensions_id']


async def read_known_ids_map(session: AsyncSession, project_id: str | uuid.UUID, field: IdField, allow_empty_text: bool = False) -> dict[str, str]:
    """
    Return a mapping from ID in field (e.g. DOI) to item_id

    Note, that this does ignore potential conflicts and always picks the "last" one.
    e.g. there might be multiple item_ids for the same DOI, but this method will effectively pick a random item_id from that set

    :param session:
    :param project_id:
    :param field:
    :param allow_empty_text:
    :return:
    """
    if field not in {'item_id', 'doi', 'wos_id', 'scopus_id', 'openalex_id', 's2_id', 'pubmed_id', 'dimensions_id'}:
        raise KeyError(f'Invalid field `{field}`')

    return dict(
        await gather_async(
            read_ids_for_project(
                session=session,
                project_id=project_id,
                field=field,
                log=logger,
                allow_empty_text=allow_empty_text,
            )
        )
    )


async def read_known_ids_map_full(session: AsyncSession, project_id: str | uuid.UUID, field: IdField) -> dict[str, set[str]]:
    """
    Return a mapping from ID in field (e.g. DOI) to item_id
    Same as `read_known_ids_map()`, but returns all matching item_ids

    :param session:
    :param project_id:
    :param field:
    :return:
    """
    if field not in {'item_id', 'doi', 'wos_id', 'scopus_id', 'openalex_id', 's2_id', 'pubmed_id', 'dimensions_id'}:
        raise KeyError(f'Invalid field `{field}`')

    known_ids = defaultdict(set)

    async for identifier, item_id in read_ids_for_project(session=session, project_id=project_id, field=field, log=logger):
        known_ids[identifier].add(item_id)

    return known_ids


async def read_ids_for_project(
    session: AsyncSession,
    log: logging.Logger,
    project_id: str | uuid.UUID,
    field: IdField,
    batch_size: int = 5000,
    allow_empty_text: bool = False,
) -> AsyncGenerator[tuple[str, str], None]:
    if field not in {'item_id', 'doi', 'wos_id', 'scopus_id', 'openalex_id', 's2_id', 'pubmed_id', 'dimensions_id'}:
        raise KeyError(f'Invalid field `{field}`')
    txt_where = '' if allow_empty_text else 'AND text IS NOT NULL'

    rslt = (
        (
            await session.stream(
                text(f"""
            SELECT aiv.{field} as identifier, aiv.item_id::text
            FROM academic_item_variant aiv
            JOIN import ON aiv.import_id = import.import_id
            WHERE import.project_id = :project_id {txt_where}

            UNION

            SELECT ai.{field} as identifier, ai.item_id::text
            FROM academic_item ai
            JOIN item i ON ai.item_id = i.item_id
            WHERE ai.project_id = :project_id {txt_where};
        """),
                {'project_id': project_id},
            )
        )
        .mappings()
        .partitions(batch_size)
    )

    async for batch in rslt:
        log.debug(f'Received batch with {len(batch)} entries.')
        for r in batch:
            yield r['identifier'], r['item_id']


async def read_item_ids_for_import(
    session: AsyncSession,
    import_id: str | uuid.UUID,
    batch_size: int = 5000,
) -> AsyncGenerator[str, None]:
    stmt = select(cast(m2m_import_item_table.c.item_id, TEXT)).where(m2m_import_item_table.c.import_id == import_id).execution_options(yield_per=batch_size)
    rslt = (await session.stream(stmt)).scalars().partitions()

    async for batch in rslt:
        for r in batch:
            yield r
