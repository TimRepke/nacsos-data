import re
import uuid
import logging
from typing import Any

from pydantic import BaseModel
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from ...db import DatabaseEngineAsync
from ...db.schemas import AcademicItem, ItemType, AcademicItemVariant, m2m_import_item_table
from ...models.items import AcademicItemModel
from ...models.items.academic import AcademicAuthorModel, AcademicItemVariantModel
from ..errors import NotFoundError
from .clean import clear_empty

logger = logging.getLogger('nacsos_data.util.academic.duplicate')

REGEX_NON_ALPH = re.compile(r'[^a-z]')
REGEX_NON_ALPHNUM = re.compile(r'[^a-z0-9]')


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
    openalex_id: str | None = None
    publication_year: int | None = None


YEAR_PATTERN = re.compile(r'\d{4}')


def _are_actually_duplicate(item: AcademicItemModel,
                            candidate: Candidate) -> bool:
    """
    Return True if the `candidate` looks like a true duplicate of `item` after looking at it more closely.
    :param item:
    :param candidate:
    :return:
    """

    tslug_item = item.title_slug if item.title_slug is not None and len(item.title_slug) > 0 else None
    tslug_cand = candidate.title_slug if candidate.title_slug is not None and len(candidate.title_slug) > 0 else None

    if tslug_item is not None and tslug_cand is not None:
        # Different non-empty title-slug means we must have matched on something else like an ID
        #   -> in that case, we will always trust IDs and stop here
        if tslug_item != tslug_cand:
            # ... actually, we don't trust DOIs too much.
            # Even though the DOIs matched, the papers seem to have different titles -> not duplicate!
            # This is likely the case of the datasource (for example) using the DOI of the book rather than the chapters.
            # But we only do this, if we get long titles, otherwise there might be too many false positive non-duplicates.
            # For example, consider "Carbon trading: What does it mean for RE?" vs "Carbon trading".
            if item.doi == candidate.doi:
                if len(tslug_item) > 20 and len(tslug_cand) > 20:
                    return False

            # We trust wos_id and co -> true duplicate
            return True

        # The non-empty title-slugs match, let's dig deeper
        if tslug_item == tslug_cand:

            # This looks like an annual report (contains year pattern), always assume not-duplicate
            if ((item.title is not None and YEAR_PATTERN.match(item.title))
                    and (candidate.title is not None and YEAR_PATTERN.match(candidate.title))):
                if REGEX_NON_ALPHNUM.sub(item.title.lower(), '') == REGEX_NON_ALPHNUM.sub(candidate.title.lower(), ''):
                    return True

                return False

            # Publication years are more than a year apart, so we don't consider that duplicate anymore
            if abs((item.publication_year or 0) - (candidate.publication_year or 0)) > 1:
                return False

            # TODO: Probably do something about authors for additional evidence
            # TODO: Maybe (only) for short title slugs

            if item.openalex_id != candidate.openalex_id:
                return True

    # We might have found this candidate because both title-slugs are empty
    elif tslug_item is None and tslug_cand is None:
        pass

    # If title slugs are different, we must have matched on something else, so fall through
    else:
        pass

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
                          check_dimensions_id: bool = False,
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
    :param check_dimensions_id:
    :param check_s2_id:
    :param session:
    :return:
    """

    assert db_engine is not None or session is not None, 'You need provide either an engine or an open session.'

    if not item.title_slug:
        item.title_slug = get_title_slug(item)

    stmt = select(AcademicItem.item_id,
                  AcademicItem.doi,
                  AcademicItem.openalex_id,
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
    if check_dimensions_id and item.dimensions_id is not None:
        checks.append(AcademicItem.dimensions_id == item.dimensions_id)
    if check_s2_id and item.s2_id is not None:
        checks.append(AcademicItem.s2_id == item.s2_id)

    # Note: We are not checking for possible alternative identifiers in
    #       the academic_item_variants table, because this could lead to runaway
    #       chains of false-positive duplicates.

    stmt = stmt.where(or_(*checks))

    if db_engine is not None:
        new_session: AsyncSession
        async with db_engine.session() as new_session:
            candidates = [Candidate.model_validate(r) for r in (await new_session.execute(stmt)).mappings().all()]
    elif session is not None:
        candidates = [Candidate.model_validate(r) for r in (await session.execute(stmt)).mappings().all()]
    else:
        raise ConnectionError('No connection to database.')

    if len(candidates) > 0:
        if check_tslug_advanced:
            return [c for c in candidates if _are_actually_duplicate(item, c)]
        return candidates

    return None


REGEX_ABSTRACT_WORD_STRIPPER = re.compile(r'(abstract|title|a|the)')
REGEX_ABSTRACT_CHAR_STRIPPER = re.compile(r'[^A-Za-z0-9 ]')


def are_abstracts_duplicate(abs1: str | None, abs2: str | None) -> bool:
    # maybe both abstracts are already trivially the same
    if abs1 == abs2:
        return True

    # apparently not, so let's simplify the abstracts
    # so that minor differences are ignored before checking again for equality

    if abs1 is not None:
        abs1 = abs1.lower()
        try:
            abs1 = REGEX_ABSTRACT_WORD_STRIPPER.sub(abs1, '')
            abs1 = REGEX_ABSTRACT_CHAR_STRIPPER.sub(abs1, '')
        except re.error:
            pass
        if len(abs1) == 0:
            abs1 = None

    if abs2 is not None:
        abs2 = abs2.lower()
        try:
            abs2 = REGEX_ABSTRACT_WORD_STRIPPER.sub(abs2, '')
            abs2 = REGEX_ABSTRACT_CHAR_STRIPPER.sub(abs2, '')
        except re.error:
            pass
        if len(abs2) == 0:
            abs2 = None

    return abs1 == abs2


def fuse_items(item1: AcademicItemModel,
               item2: AcademicItemModel,
               fuse_authors: bool = True,
               fuse_keywords: bool = True) -> AcademicItemModel:
    """
    This method fuses the data from two items together.
    When in doubt, it will prefer values from the first item over those in the second.
    For many attributes, it will try to find the best fit or even merge data.

    :param fuse_authors: if True, fuse lists of authors; if False: just take author list from item1 (if available)
    :param fuse_keywords: if True, fuse lists of keywords; if False: just take keywords from item1 (if available)
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
    item.dimensions_id = _pick_best_str('dimensions_id')

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

    # TODO: Do we want to be more considerate and do recursive updating?
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
    elif item1.text is not None:  # type: ignore[unreachable]
        item.text = item1.text
    elif item2.text is not None:
        item.text = item2.text

    # TODO: Try de-duplicating keywords
    if not fuse_keywords and item1.keywords is not None and len(item1.keywords) > 0:
        item.keywords = item1.keywords
    elif item1.keywords is not None and item2.keywords is not None:
        item.keywords = list(set(item1.keywords).union(set(item2.keywords)))
    elif item1.keywords is not None:
        item.keywords = item1.keywords
    elif item2.keywords is not None:
        item.keywords = item2.keywords

    if not fuse_authors and item1.authors is not None and len(item1.authors) > 0:
        item.authors = item1.authors
    else:
        authors_: list[AcademicAuthorModel] = (item1.authors or []) + (item2.authors or [])
        authors: dict[str, AcademicAuthorModel] = {}
        for author in authors_:
            if author.name not in authors:  # TODO: There's probably a better way to match authors than by literal string match
                authors[author.name] = author
            else:
                if authors[author.name].surname_initials is None:
                    if author.surname_initials is not None and len(author.surname_initials) == 0:
                        authors[author.name].surname_initials = author.surname_initials

                authors[author.name].surname_initials = _pick_best_str_('surname_initials', authors[author.name],
                                                                        author)
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


def _safe_lower(s: str | None) -> str | None:
    if s is not None:
        return s.lower().strip()
    return None


async def duplicate_insertion(new_item: AcademicItemModel,
                              orig_item_id: str | uuid.UUID,
                              import_id: str | uuid.UUID | None,
                              trust_new_authors: bool,
                              trust_new_keywords: bool,
                              session: AsyncSession,
                              log: logging.Logger | None = None) -> None:
    """
    This method handles insertion of an item for which we found a duplicate in the database with `item_id`

    :param log:
    :param trust_new_keywords: if True, won't try to fuse list of keywords but just take them from `new_item` instead
    :param trust_new_authors: if True, won't try to fuse list of authors but just take them from `new_item` instead
    :param import_id:
    :param session:
    :param new_item:
    :param orig_item_id: id in academic_item of which the `new_item` is a duplicate
    :return:
    """
    if log is None:
        log = logger

    # Fetch the original item from the database
    orig_item_orm = await session.get(AcademicItem, {'item_id': orig_item_id})

    # This should never happen, but let's check just in case
    if orig_item_orm is None:
        raise NotFoundError(f'No item found for {orig_item_id}')

    orig_item = AcademicItemModel.model_validate(orig_item_orm.__dict__)

    # Get prior variants of that AcademicItem
    variants = (await session.scalars(select(AcademicItemVariant)
                                      .where(AcademicItemVariant.item_id == orig_item_id))).all()

    # If we have no prior variant, we need to create one
    if len(variants) == 0:
        # For the first variant, we need to fetch the original import_id
        orig_import_id = await session.scalar(select(m2m_import_item_table.c.import_id)
                                              .where(m2m_import_item_table.c.item_id == orig_item_id))
        # Note, we are not checking for "not None", because it might be a valid case where no import_id exists

        variant = AcademicItemVariantModel(
            item_variant_id=uuid.uuid4(),
            item_id=orig_item_id,
            import_id=orig_import_id,
            doi=orig_item.doi,
            wos_id=orig_item.wos_id,
            scopus_id=orig_item.scopus_id,
            openalex_id=orig_item.openalex_id,
            s2_id=orig_item.s2_id,
            pubmed_id=orig_item.pubmed_id,
            dimensions_id=orig_item.dimensions_id,
            title=orig_item.title,
            publication_year=orig_item.publication_year,
            source=orig_item.source,
            keywords=orig_item.keywords,
            authors=orig_item.authors,
            abstract=orig_item.text,
            meta=orig_item.meta)

        # add to database
        session.add(AcademicItemVariant(**variant.model_dump()))
        await session.flush()

        log.debug(f'Created first variant of item {orig_item_id} at {variant.item_variant_id}')
        # use this new variant for further value thinning
        variants = [variant]  # type: ignore[list-item]

    new_variant = AcademicItemVariantModel(
        item_variant_id=uuid.uuid4(),
        item_id=orig_item_id,
        import_id=import_id,
        doi=new_item.doi,
        wos_id=new_item.wos_id,
        scopus_id=new_item.scopus_id,
        openalex_id=new_item.openalex_id,
        s2_id=new_item.s2_id,
        pubmed_id=new_item.pubmed_id,
        dimensions_id=new_item.dimensions_id,
        title=new_item.title,
        publication_year=new_item.publication_year,
        source=new_item.source,
        keywords=new_item.keywords,
        authors=new_item.authors,
        abstract=new_item.text,
        meta=new_item.meta)

    # if we've seen this abstract before, drop it to save memory
    if any([are_abstracts_duplicate(new_item.text, var.abstract) for var in variants]):
        new_variant.abstract = None
    # if we've seen this doi before, drop it
    if any([new_item.doi == var.doi for var in variants]):
        new_variant.doi = None
    # if we've seen this wos_id before, drop it
    if any([new_item.wos_id == var.wos_id for var in variants]):
        new_variant.wos_id = None
    # if we've seen this scopus_id before, drop it
    if any([new_item.scopus_id == var.scopus_id for var in variants]):
        new_variant.scopus_id = None
    # if we've seen this openalex_id before, drop it
    if any([new_item.openalex_id == var.openalex_id for var in variants]):
        new_variant.openalex_id = None
    # if we've seen this s2_id before, drop it
    if any([new_item.s2_id == var.s2_id for var in variants]):
        new_variant.s2_id = None
    # if we've seen this pubmed_id before, drop it
    if any([new_item.pubmed_id == var.pubmed_id for var in variants]):
        new_variant.pubmed_id = None
    # if we've seen this dimensions_id before, drop it
    if any([new_item.dimensions_id == var.dimensions_id for var in variants]):
        new_variant.dimensions_id = None
    # if we've seen this title before, drop it
    if any([_safe_lower(new_item.title) == _safe_lower(var.title) for var in variants]):
        new_variant.title = None
    # if we've seen this publication_year before, drop it
    if any([new_item.publication_year == var.publication_year for var in variants]):
        new_variant.publication_year = None
    # if we've seen this source before, drop it
    if any([new_item.source == var.source for var in variants]):
        new_variant.source = None

    log.debug(f'Found {len(variants or [])} variants and adding one more.')

    session.add(AcademicItemVariant(**new_variant.model_dump()))
    await session.flush()

    # Fuse all the fields from both, the existing and new variant into a new item
    fused_item = fuse_items(item1=new_item,
                            item2=orig_item,
                            fuse_authors=not trust_new_authors,
                            fuse_keywords=not trust_new_keywords)

    # Python seems to drop the ORM for some reason, so fetch us a fresh copy of the original item
    orig_item_orm = await session.get(AcademicItem, {'item_id': orig_item_id})
    if orig_item_orm is None:  # if this happens, something went horribly wrong. But at least mypy is happy.
        raise RuntimeError(f'No item found for {orig_item_id}')

    # Partially update the fields in the database that changed after fusion
    if fused_item.doi != orig_item_orm.doi:
        orig_item_orm.doi = fused_item.doi
    if fused_item.wos_id != orig_item_orm.wos_id:
        orig_item_orm.wos_id = fused_item.wos_id
    if fused_item.scopus_id != orig_item_orm.scopus_id:
        orig_item_orm.scopus_id = fused_item.scopus_id
    if fused_item.openalex_id != orig_item_orm.openalex_id:
        orig_item_orm.openalex_id = fused_item.openalex_id
    if fused_item.s2_id != orig_item_orm.s2_id:
        orig_item_orm.s2_id = fused_item.s2_id
    if fused_item.pubmed_id != orig_item_orm.pubmed_id:
        orig_item_orm.pubmed_id = fused_item.pubmed_id
    if fused_item.dimensions_id != orig_item_orm.dimensions_id:
        orig_item_orm.dimensions_id = fused_item.dimensions_id
    if fused_item.title != orig_item_orm.title:
        orig_item_orm.title = fused_item.title
    if fused_item.title_slug != orig_item_orm.title_slug:
        orig_item_orm.title_slug = fused_item.title_slug
    if fused_item.publication_year != orig_item_orm.publication_year:
        orig_item_orm.publication_year = fused_item.publication_year
    if fused_item.source != orig_item_orm.source:
        orig_item_orm.source = fused_item.source
    if fused_item.text != orig_item_orm.text:
        orig_item_orm.text = fused_item.text
    # here we don't check and just do
    orig_item_orm.meta = fused_item.meta
    orig_item_orm.keywords = fused_item.keywords
    orig_item_orm.authors = fused_item.authors

    # commit the changes
    await session.flush()
