import re
import uuid
import logging
from datetime import date

from pydantic import BaseModel
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from .. import fuze_dicts
from ...db import DatabaseEngineAsync
from ...db.schemas import AcademicItem, AcademicItemVariant, m2m_import_item_table
from ...models.items import AcademicItemModel
from ...models.items.academic import AcademicItemVariantModel
from ..errors import NotFoundError
from .clean import clear_empty

logger = logging.getLogger('nacsos_data.util.academic.duplicate')

REGEX_NON_ALPH = re.compile(r'[^a-z]')
REGEX_NON_ALPHNUM = re.compile(r'[^a-z0-9]')
LATEST_POSSIBLE_PUB_YEAR = date.today().year + 5
# determined by case study at https://gitlab.pik-potsdam.de/mcc-apsis/nacsos/case-studies/duplicate-detection/-/blob/main/202407_experiments/avg_abstract_len.sql?ref_type=heads
MAX_ABSTRACT_LENGTH = 10000


def str_to_title_slug(title: str | None) -> str | None:
    print(title)
    if title is None or len(title.strip()) == 0:
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


def _safe_lower(s: str | None) -> str | None:
    if s is not None:
        return s.lower().strip()
    return None


async def duplicate_insertion(new_item: AcademicItemModel,
                              orig_item_id: str | uuid.UUID,
                              import_id: str | uuid.UUID | None,
                              import_revision: int | None,
                              session: AsyncSession,
                              log: logging.Logger | None = None) -> bool:
    """
    This method handles insertion of an item for which we found a duplicate in the database with `item_id`

    :param log:
    :param import_id:
    :param session:
    :param new_item:
    :param orig_item_id: id in academic_item of which the `new_item` is a duplicate
    :return: Return True if we ended up creating a new variant.
    """
    if log is None:
        log = logger

    # Fetch the original item from the database
    orig_item_orm = await session.get(AcademicItem, {'item_id': orig_item_id})

    # This should never happen, but let's check just in case
    if orig_item_orm is None:
        raise NotFoundError(f'No item found for {orig_item_id}')

    orig_item = AcademicItemModel.model_validate(orig_item_orm.__dict__)
    editable = orig_item_orm.time_edited is None

    # Get prior variants of that AcademicItem
    variants = [v.__dict__
                for v in (await session.execute(select(AcademicItemVariant)
                                                .where(AcademicItemVariant.item_id == orig_item_id))).scalars().all()]

    # If we have no prior variant, we need to create one
    if len(variants) == 0:
        # For the first variant, we need to fetch the original import_id
        orig_import = (await session.execute(select(m2m_import_item_table.c.import_id, m2m_import_item_table.c.latest_revision)
                                             .where(m2m_import_item_table.c.item_id == orig_item_id))).mappings().one_or_none()
        # Note, we are not checking for "not None", because it might be a valid case where no import_id exists
        variant = AcademicItemVariantModel(
            item_variant_id=uuid.uuid4(),
            item_id=orig_item_id,
            import_id=(orig_import or {})['import_id'],  # type: ignore[index]
            import_revision=(orig_import or {})['latest_revision'],  # type: ignore[index]
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
            text=orig_item.text,
            meta=orig_item.meta)

        # add to database
        session.add(AcademicItemVariant(**variant.model_dump()))
        await session.flush()

        log.debug(f'Created first variant of item {orig_item_id} at {variant.item_variant_id}')
        # use this new variant for further value thinning
        variants = [variant.model_dump()]  # type: ignore[list-item]

    # Object to keep track of previously unseen values
    new_variant = {}

    # Check ID fields, title, abstract, and source (aka venue/journal)
    for field in ['doi', 'wos_id', 'scopus_id', 'openalex_id', 's2_id', 'pubmed_id', 'dimensions_id', 'source', 'title', 'text']:
        # If the new item value is empty, we have noting else to do
        if getattr(new_item, field) is None:
            continue

        # Cleaned value from the new item for `field`
        new_value = getattr(new_item, field).strip()
        # Get all non-empty values for the field across variants
        field_values = set([var[field].strip() for var in variants if var[field] is not None and len(var[field].strip()) > 0])

        # Check if we have seen this value before
        if new_value not in field_values:
            if field == 'title':
                new_variant[field] = new_value
                if editable:
                    setattr(orig_item_orm, field, new_value)
                    setattr(orig_item_orm, 'title_slug', str_to_title_slug(new_value))
            elif field == 'text':
                candidates = sorted([abs for abs in field_values | {new_value} if len(abs) < MAX_ABSTRACT_LENGTH], key=lambda a: len(a))
                new_variant[field] = new_value
                if len(candidates) > 0 and editable:
                    setattr(orig_item_orm, field, candidates[-1])
            else:
                # This was new, so keep track of it in our variant
                new_variant[field] = new_value
                if editable:
                    # We always like new IDs, so update the reference item
                    setattr(orig_item_orm, field, new_value)

    # Check publication year field
    new_pub_year = getattr(new_item, 'publication_year')
    if new_pub_year is not None:
        # Get all non-empty publication_year across variants
        pub_yrs = set([var['publication_year'] for var in variants if var['publication_year'] is not None])

        # Check if we have seen this value before
        if new_pub_year not in pub_yrs:
            # This was new, so keep track of it in our variant
            new_variant['publication_year'] = new_pub_year
            if editable:
                # We always like new years, so update the reference item
                setattr(orig_item_orm, 'publication_year', min(LATEST_POSSIBLE_PUB_YEAR, max(pub_yrs | {new_pub_year})))

    # Check publication year field
    new_keywords = getattr(new_item, 'keywords')
    if new_keywords is not None and len(new_keywords) > 0:
        # Get all non-empty publication_year across variants
        keywords = set([var['keywords'] for var in variants if var['keywords'] is not None])

        # Check if we have seen this value before
        if new_keywords not in keywords:
            # This was new, so keep track of it in our variant
            new_variant['keywords'] = new_keywords
            if editable:
                # We always like new IDs, so update the reference item
                setattr(orig_item_orm, 'keywords', [kw for kw_lst in keywords for kw in kw_lst])

    # Checking metadata field
    # only keep track of unique meta objects in variants
    # always apply deep-fuzed meta objects when encountering new meta object
    new_meta = getattr(new_item, 'meta')
    if new_meta is not None and len(new_meta) > 0:
        # Get all non-empty publication_year across variants
        metas = [var['meta'] for var in variants if var['meta'] is not None and len(var['meta']) > 0]

        # Check if we have seen this value before
        if new_meta not in metas:
            # This was new, so keep track of it in our variant
            new_variant['meta'] = new_meta
            if editable:
                # We always like new IDs, so update the reference item
                setattr(orig_item_orm, 'meta', clear_empty(fuze_dicts(getattr(orig_item_orm, 'meta'), new_meta)))

    # Checking authorships
    # only keep track of unique list of authors (or variations thereof) in variants
    # always keep last valid list of authors
    new_authors = getattr(new_item, 'authors')
    if new_authors is not None and len(new_authors) > 0:
        # Get all non-empty publication_year across variants
        authors = [var['authors'] for var in variants if var['authors'] is not None and len(var['authors']) > 0]

        # Check if we have seen this value before
        if new_authors not in authors:
            # This was new, so keep track of it in our variant
            new_variant['authors'] = new_authors
            if editable:
                # We always like new IDs, so update the reference item
                setattr(orig_item_orm, 'authors', new_authors)

    log.debug(f'Duplicate checking revealed new field variants for {len(new_variant)} fields: {new_variant.keys()}.')

    if len(new_variant) > 0:
        new_variant_db = AcademicItemVariant(**new_variant)
        new_variant_db.item_variant_id = uuid.uuid4()
        new_variant_db.import_id = import_id
        new_variant_db.import_revision = import_revision
        new_variant_db.item_id = orig_item_id
        session.add(new_variant_db)

        # commit the changes
        await session.flush()

        return True

    return False
