import datetime
import logging
import uuid
from typing import Generator

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401
from sqlalchemy.exc import IntegrityError
from psycopg.errors import UniqueViolation

from ...db.engine import DatabaseEngineAsync
from ...db.schemas import Import, AcademicItem, m2m_import_item_table
from ...db.schemas.items.academic import AcademicItemVariant
from ...models.imports import ImportType, M2MImportItemType
from ...models.items import AcademicItemModel, AcademicItemVariantModel
from ..errors import NotFoundError
from .clean import get_cleaned_meta_field
from .duplicate import str_to_title_slug, find_duplicates, are_abstracts_duplicate, fuse_items

logger = logging.getLogger('nacsos_data.util.academic.import')


async def import_academic_items(
        items: list[AcademicItemModel] | Generator[AcademicItemModel, None, None],
        project_id: str | uuid.UUID,
        db_engine: DatabaseEngineAsync,
        import_name: str | None = None,
        import_id: str | uuid.UUID | None = None,
        user_id: str | uuid.UUID | None = None,
        description: str | None = None,
        dry_run: bool = True,
        trust_new_authors: bool = False,
        trust_new_keywords: bool = False,
) -> tuple[str | uuid.UUID, list[str | uuid.UUID]]:
    """
    Helper function for programmatically importing `AcademicItem`s into the platform.

    Example usage:
    ```
    from nacsos_data.db import get_engine_async
    from nacsos_data.util.academic.scopus import read_scopus_file
    from nacsos_data.util.academic.import import import_academic_items

    PROJECT_ID = '??'

    db_engine = get_engine_async(conf_file='/path/to/remote_config.env')
    scopus_works = read_scopus_file('/path/to/scopus.csv', project_id=PROJECT_ID)
    await import_academic_items(items=scopus_works, db_engine=db_engine, project_id=PROJECT_ID, ...)
    ```

    If you are working in a synchronous context, you can wrap the above code in a method and run with asyncio:
    ```
    import asyncio

    def main():
        ...

    if __name__ == '__main__':
        asyncio.run(main())
    ```

    Items are always associated with a project, and within a project with an `Import`.
    This is used to indicate a "scope" of where the data comes from, e.g. a query from WoS or Scopus.
    Item sets may overlap between Imports.

    There are two modes:
      1) You can use an existing Import by providing the `import_id`.
         This might be useful when you already created a blank import via the WebUI or
         want to add more items to that import. Note, that we recommend creating a new import if
         the "scopes" are better semantically separate (e.g. query results from different points in time might be
         two Imports rather than one Import that is added to).
         In this case `user_id`, `import_name`, `description` are ignored.
      2) Create a new Import by setting `user_id`, `import_name`, and `description`; optionally set `import_id`.
         In this way, a new Import will be created and all items will be associated with that.


    :param trust_new_authors:
    :param trust_new_keywords:
    :param items: A list (or generator) of AcademicItems
    :param project_id: ID of the project the items should be added to
    :param import_id: (optional) ID to existing Import
    :param user_id: (your) user_id, which this import will be associated with
    :param import_name: Concise and descriptive name for this import
    :param description: Proper (markdown) description for this import.
                        Usually this should describe the source of the dataset and, if applicable, the search query.
    :param dry_run: If false, actually write data to the database;
                    If true, simulate best as possible (note, that duplicates within the `items` are not validated
                                                        and not all constraints can be checked)
    :param db_engine: an async database engine
    :return: import_id and list of item_ids that were actually used in the end
    """
    if project_id is None:
        raise AttributeError('You have to provide a project ID!')

    if items is None:
        raise AttributeError('You have to provide data!')

    item_ids = []

    async with db_engine.session() as session:  # type: AsyncSession
        if import_name is not None:
            if description is None or user_id is None:
                raise AttributeError('You need to provide a meaningful description and a user id!')

            if import_id is None:
                import_id = uuid.uuid4()

            import_orm = Import(
                project_id=project_id,
                user_id=user_id,
                import_id=import_id,
                name=import_name,
                description=description,
                type=ImportType.script,
                time_created=datetime.datetime.now()
            )
            if dry_run:
                logger.info('I will create a new `Import`!')
            else:
                session.add(import_orm)
                await session.commit()
                logger.info(f'Created new import with ID {import_id}')

        elif import_id is not None:
            # check that the uuid actually exists...
            import_orm = await session.get(Import, {'import_id': import_id})  # type: ignore[assignment]
            if import_orm is None:
                raise KeyError('No import found for the given ID!')
            if str(import_orm.project_id) != str(project_id):
                raise AssertionError(f'The project ID does not match with the `Import` you provided: '
                                     f'"{import_orm.project_id}" vs "{project_id}"')

            logger.info(f'Using existing import with ID {import_id}')

        else:
            raise AttributeError('Seems like neither provided information for creating '
                                 'a new import nor the ID to an existing import!')

        # Keep track of when we started importing
        import_orm.time_started = datetime.datetime.now()

        for item in items:
            logger.info(f'Importing AcademicItem with doi {item.doi} and title "{item.title}"')

            # remove empty entries from the meta-data field
            item.meta = get_cleaned_meta_field(item)

            # ensure we have a title_slug
            if item.title_slug is None or len(item.title_slug) == 0:
                item.title_slug = str_to_title_slug(item.title)

            duplicates = await find_duplicates(item=item,
                                               project_id=str(project_id),
                                               check_tslug=True,
                                               check_tslug_advanced=True,
                                               check_doi=True,
                                               check_wos_id=True,
                                               check_scopus_id=True,
                                               check_oa_id=True,
                                               check_pubmed_id=True,
                                               check_s2_id=True,
                                               session=session)

            try:
                if duplicates is not None and len(duplicates) > 0:
                    item_id = duplicates[0].item_id
                    if dry_run:
                        logger.debug(f'  -> There are at least {len(duplicates)}; I will probably use {item_id}')
                    else:
                        logger.debug(f' -> Has {len(duplicates)} duplicates; using {item_id}.')
                        if item.item_id is None:
                            item.item_id = uuid.uuid4()
                        await duplicate_insertion(orig_item_id=item_id,
                                                  import_id=import_id,
                                                  new_item=item,
                                                  trust_new_authors=trust_new_authors,
                                                  trust_new_keywords=trust_new_keywords,
                                                  session=session)
                else:
                    if dry_run:
                        logger.debug('  -> I will create a new AcademicItem!')
                    else:
                        item_id = str(uuid.uuid4())
                        logger.debug(f' -> Creating new item with ID {item_id}!')
                        item.item_id = item_id
                        session.add(AcademicItem(**item.dict()))
                        await session.commit()

                if dry_run:
                    logger.debug('  -> I will create an m2m entry.')
                else:
                    item_ids.append(item_id)
                    stmt_m2m = insert(m2m_import_item_table) \
                        .values(item_id=item_id, import_id=import_id, type=M2MImportItemType.explicit)
                    try:
                        await session.execute(stmt_m2m)
                        await session.commit()
                        logger.debug(' -> Added many-to-many relationship for import/item')
                    except IntegrityError:
                        logger.debug(f' -> M2M_i2i already exists, ignoring {import_id} <-> {item_id}')
                        await session.rollback()

            except (UniqueViolation, IntegrityError) as e:
                logger.exception(e)
                await session.rollback()

        # Keep track of when we finished importing
        import_orm.time_finished = datetime.datetime.now()
        await session.commit()

    return import_id, item_ids


def _safe_lower(s: str | None) -> str | None:
    if s is not None:
        return s.lower().strip()
    return None


async def duplicate_insertion(new_item: AcademicItemModel,
                              orig_item_id: str | uuid.UUID,
                              import_id: str | uuid.UUID | None,
                              trust_new_authors: bool,
                              trust_new_keywords: bool,
                              session: AsyncSession) -> None:
    """
    This method handles insertion of an item for which we found a duplicate in the database with `item_id`

    :param trust_new_keywords: if True, won't try to fuse list of keywords but just take them from `new_item` instead
    :param trust_new_authors: if True, won't try to fuse list of authors but just take them from `new_item` instead
    :param import_id:
    :param session:
    :param new_item:
    :param orig_item_id: id in academic_item of which the `new_item` is a duplicate
    :return:
    """

    # Fetch the original item from the database
    orig_item_orm = await session.get(AcademicItem, {'item_id': orig_item_id})

    # This should never happen, but let's check just in case
    if orig_item_orm is None:
        raise NotFoundError(f'No item found for {orig_item_id}')

    orig_item = AcademicItemModel.parse_obj(orig_item_orm.__dict__)

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
            title=orig_item.title,
            publication_year=orig_item.publication_year,
            source=orig_item.source,
            keywords=orig_item.keywords,
            authors=orig_item.authors,
            abstract=orig_item.text,
            meta=orig_item.meta)

        # add to database
        session.add(AcademicItemVariant(**variant.dict()))
        await session.commit()

        logger.debug(f'Created first variant of item {orig_item_id} at {variant.item_variant_id}')
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
    # if we've seen this title before, drop it
    if any([_safe_lower(new_item.title) == _safe_lower(var.title) for var in variants]):
        new_variant.title = None
    # if we've seen this publication_year before, drop it
    if any([new_item.publication_year == var.publication_year for var in variants]):
        new_variant.publication_year = None
    # if we've seen this source before, drop it
    if any([new_item.source == var.source for var in variants]):
        new_variant.source = None

    logger.debug(f'Found {len(variants or [])} variants and adding one more.')

    session.add(AcademicItemVariant(**new_variant.dict()))
    await session.commit()

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
    await session.commit()