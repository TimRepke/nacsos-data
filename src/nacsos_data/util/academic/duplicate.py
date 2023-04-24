import re
from uuid import UUID
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import AcademicItem
from nacsos_data.models.items import AcademicItemModel

REGEX_NON_ALPH = re.compile(r'[^a-z]')


def get_title_slug(item: AcademicItemModel) -> str | None:
    if item.title:
        # remove all non-alphabetic characters
        return REGEX_NON_ALPH.sub('', item.title.lower())

    # FIXME: should we rather raise an error or go for fallbacks?
    return None


async def find_duplicates(item: AcademicItemModel,
                          project_id: str | None = None,
                          check_tslug: bool = False,
                          check_doi: bool = False,
                          check_wos_id: bool = False,
                          check_oa_id: bool = False,
                          db_engine: DatabaseEngineAsync | None = None,
                          session: AsyncSession | None = None) -> list[str] | None:
    """
    Checks in the database, if there is a duplicate of this AcademicItem.
    Optionally (if `project_id` is not None), this will search only within one project.
    Optionally, you can also include the DOI or Web of Science ID in the duplicate condition.
    You can provide an open session or an engine to create a session from.

    :param item:
    :param project_id:
    :param db_engine:
    :param check_tslug:
    :param check_doi:
    :param check_wos_id:
    :param check_oa_id:
    :param session:
    :return:
    """

    assert db_engine is not None or session is not None, 'You need provide either an engine or an open session.'

    if not item.title_slug:
        item.title_slug = get_title_slug(item)

    stmt = select(AcademicItem.item_id)

    if project_id is not None:
        stmt = stmt.where(AcademicItem.project_id == project_id)

    checks = []
    if check_tslug and item.title_slug is not None and len(item.title_slug) > 0:
        checks.append(AcademicItem.title_slug == item.title_slug)
    if check_doi and item.doi is not None:
        checks.append(AcademicItem.doi == item.doi)
    if check_wos_id and item.wos_id is not None:
        checks.append(AcademicItem.wos_id == item.wos_id)
    if check_oa_id and item.openalex_id is not None:
        checks.append(AcademicItem.openalex_id == item.openalex_id)
    stmt = stmt.where(or_(*checks))

    if db_engine is not None:
        async with db_engine.session() as new_session:  # type: AsyncSession
            tmp = await new_session.execute(stmt)
            item_ids: list[UUID] = tmp.scalars().all()  # type: ignore[assignment]
    elif session is not None:
        item_ids = (await session.execute(stmt)).scalars().all()  # type: ignore[assignment]
    else:
        raise ConnectionError('No connection to database.')

    if len(item_ids) > 0:
        return [str(iid) for iid in item_ids]

    return None
