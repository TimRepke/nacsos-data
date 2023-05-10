import re
import uuid
import logging
from typing import Any

from pydantic import BaseModel
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import AcademicItem, ItemType
from nacsos_data.models.items import AcademicItemModel
from nacsos_data.models.items.academic import AcademicAuthorModel
from nacsos_data.util.academic.clean import clear_empty

logger = logging.getLogger('nacsos_data.util.academic.duplicate')

REGEX_NON_ALPH = re.compile(r'[^a-z]')


def str_to_title_slug(title: str | None) -> str | None:
    if title is None or len(title) == 0:
        return None
    # remove all non-alphabetic characters
    return REGEX_NON_ALPH.sub('', title.lower())


def get_title_slug(item: AcademicItemModel) -> str | None:
    return str_to_title_slug(item.title)


class Candidate(BaseModel):
    item_id: str | uuid.UUID
    title: str | None = None
    title_slug: str | None = None
    doi: str | None = None
    publication_year: int | None = None


YEAR_PATTERN = re.compile(r'\d{4}')


def are_actually_duplicate(item: AcademicItemModel,
                           candidate: Candidate) -> bool:
    """
    Return True if the `candidate` looks like a true duplicate of `item` after looking at it more closely.
    :param item:
    :param candidate:
    :return:
    """
    # Different non-empty slug means we must have matched on something else like an ID
    #   -> in that case, we will always trust IDs and stop here
    if (item.title_slug is not None and len(item.title_slug) > 0
            and candidate.title_slug is not None and len(candidate.title_slug) > 0
            and item.title_slug != candidate.title_slug):
        # ... actually, we don't trust DOIs too much.
        # Even though the DOIs matched, the papers seem to have different titles -> not duplicate!
        # This is likely the case of the datasource (for example) using the DOI of the book rather than the chapters.
        if item.doi == candidate.doi:
            return False

        # We trust wos_id and co -> true duplicate
        return True

    # The non-empty title slugs match, let's dig deeper
    if (item.title_slug is not None and len(item.title_slug) > 0
            and candidate.title_slug is not None and len(candidate.title_slug) > 0
            and item.title_slug == candidate.title_slug):

        # This looks like an annual report (contains year pattern), always assume not-duplicate
        if ((item.title is not None and YEAR_PATTERN.match(item.title))
                or (candidate.title is not None and YEAR_PATTERN.match(candidate.title))):
            return False

        # Publication years are more than a year apart, so we don't consider that duplicate anymore
        if abs((item.publication_year or 0) - (candidate.publication_year or 0)) > 1:
            return False

    # All else failed, so let's assume it's duplicate.
    return True


async def find_duplicates(item: AcademicItemModel,
                          project_id: str | None = None,
                          check_tslug: bool = False,
                          check_tslug_advanced: bool = False,
                          check_doi: bool = False,
                          check_wos_id: bool = False,
                          check_oa_id: bool = False,
                          check_pubmed_id: bool = False,
                          check_scopus_id: bool = False,
                          check_s2_id: bool = False,
                          db_engine: DatabaseEngineAsync | None = None,
                          session: AsyncSession | None = None) -> list[Candidate] | None:
    """
    Checks in the database, if there is a duplicate of this AcademicItem.
    Optionally (if `project_id` is not None), this will search only within one project.
    Optionally, you can also include the DOI or Web of Science ID in the duplicate condition.
    You can provide an open session or an engine to create a session from.

    :param item:
    :param project_id:
    :param db_engine:
    :param check_tslug:
    :param check_tslug_advanced: do some more fine-grained duplicate detection post-processing
    :param check_doi:
    :param check_wos_id:
    :param check_oa_id:
    :param check_scopus_id:
    :param check_pubmed_id:
    :param check_s2_id:
    :param session:
    :return:
    """

    assert db_engine is not None or session is not None, 'You need provide either an engine or an open session.'

    if not item.title_slug:
        item.title_slug = get_title_slug(item)

    stmt = select(AcademicItem.item_id,
                  AcademicItem.doi,
                  AcademicItem.publication_year,
                  AcademicItem.title,
                  AcademicItem.title_slug)

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
    if check_scopus_id and item.scopus_id is not None:
        checks.append(AcademicItem.scopus_id == item.scopus_id)
    if check_pubmed_id and item.pubmed_id is not None:
        checks.append(AcademicItem.pubmed_id == item.pubmed_id)
    if check_s2_id and item.s2_id is not None:
        checks.append(AcademicItem.s2_id == item.s2_id)

    stmt = stmt.where(or_(*checks))

    if db_engine is not None:
        async with db_engine.session() as new_session:  # type: AsyncSession
            candidates = [Candidate.parse_obj(r) for r in (await new_session.execute(stmt)).mappings().all()]
    elif session is not None:
        candidates = [Candidate.parse_obj(r) for r in (await session.execute(stmt)).mappings().all()]
    else:
        raise ConnectionError('No connection to database.')

    if len(candidates) > 0:
        if check_tslug_advanced:
            return [c for c in candidates if are_actually_duplicate(item, c)]
        return candidates

    return None


def fuse_items(item1: AcademicItemModel, item2: AcademicItemModel) -> AcademicItemModel:
    """
    This method fuses the data from two items together.
    When in doubt, it will prefer values from the first item over those in the second.
    For many attributes, it will try to find the best fit or even merge data.

    :param item1:
    :param item2:
    :return:
    """
    item = AcademicItemModel(
        type=ItemType.academic,
        item_id=item1.item_id or item2.item_id or uuid.uuid4()
    )

    def _pick_best_str_(field: str, o1: object, o2: object) -> str | None:
        if hasattr(o1, field):
            val1: str | None = getattr(o1, field)
            if val1 is not None and len(val1) > 0:
                return val1

        if hasattr(o2, field):
            val2: str | None = getattr(o2, field)
            if val2 is not None and len(val2) > 0:
                return val2

        return None

    def _pick_best_str(field: str) -> str | None:
        return _pick_best_str_(field, item1, item2)

    item.doi = _pick_best_str('doi')
    item.wos_id = _pick_best_str('wos_id')
    item.scopus_id = _pick_best_str('scopus_id')
    item.openalex_id = _pick_best_str('openalex_id')
    item.s2_id = _pick_best_str('s2_id')
    item.pubmed_id = _pick_best_str('pubmed_id')

    # TODO: Do something more sensible than picking any title in order
    item.title = _pick_best_str('title')
    item.title_slug = str_to_title_slug(item.title)
    item.source = _pick_best_str('source')

    if item1.publication_year is not None and item2.publication_year is not None:
        item.publication_year = max(item1.publication_year, item2.publication_year)
    elif item1.publication_year is not None:
        item.publication_year = item1.publication_year
    elif item2.publication_year is not None:
        item.publication_year = item2.publication_year

    meta1: dict[str, Any] | None = clear_empty(item1.meta)
    meta2: dict[str, Any] | None = clear_empty(item2.meta)
    if meta1 is not None and meta2 is not None:
        item.meta = meta2.update(meta1)
    elif meta1 is not None:
        item.meta = meta1
    elif meta2 is not None:
        item.meta = meta2

    if item1.text is not None and item2.text is not None:
        # TODO: Do something more sensible than taking the longer abstract
        if len(item1.text) > len(item2.text):
            item.text = item1.text
        else:
            item.text = item2.text
    elif item1.text is not None:
        item.text = item1.text
    elif item2.text is not None:
        item.text = item2.text

    # TODO: Try de-duplicating keywords
    if item1.keywords is not None and item2.keywords is not None:
        item.keywords = list(set(item1.keywords).union(set(item2.keywords)))
    elif item1.keywords is not None:
        item.keywords = item1.keywords
    elif item2.keywords is not None:
        item.keywords = item2.keywords

    authors_: list[AcademicAuthorModel] = (item1.authors or []) + (item2.authors or [])
    authors: dict[str, AcademicAuthorModel] = {}
    for author in authors_:
        if author.name not in authors:  # TODO: There's probably a better way to match authors than by literal string match
            authors[author.name] = author
        else:
            if authors[author.name].surname_initials is None:
                if author.surname_initials is not None and len(author.surname_initials) == 0:
                    authors[author.name].surname_initials = author.surname_initials

            authors[author.name].surname_initials = _pick_best_str_('surname_initials', authors[author.name], author)
            authors[author.name].email = _pick_best_str_('email', authors[author.name], author)
            authors[author.name].orcid = _pick_best_str_('orcid', authors[author.name], author)
            authors[author.name].scopus_id = _pick_best_str_('scopus_id', authors[author.name], author)
            authors[author.name].openalex_id = _pick_best_str_('openalex_id', authors[author.name], author)
            authors[author.name].s2_id = _pick_best_str_('s2_id', authors[author.name], author)

            aff1 = authors[author.name].affiliations if len(authors[author.name].affiliations or []) > 0 else None
            aff2 = author.affiliations if len(author.affiliations or []) > 0 else None
            if aff1 is not None and aff2 is not None:
                # TODO: There's probably a better way to match affiliations than by literal string match
                authors[author.name].affiliations = list({aff.name: aff for aff in aff1 + aff2}.values())
            elif aff1 is None and aff2 is not None:
                authors[author.name].affiliations = aff2
            elif aff1 is not None and aff2 is None:
                authors[author.name].affiliations = aff1
            else:
                authors[author.name].affiliations = None

    item.authors = list(authors.values())

    return item
