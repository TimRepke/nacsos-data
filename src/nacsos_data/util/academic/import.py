import datetime
import logging
import uuid
from typing import Generator

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401
from sqlalchemy.exc import IntegrityError
from psycopg.errors import UniqueViolation

from nacsos_data.db.schemas import Import, AcademicItem, m2m_import_item_table
from nacsos_data.models.imports import ImportType, M2MImportItemType
from ...db.engine import DatabaseEngineAsync
from ...models.items import AcademicItemModel

from .duplicate import str_to_title_slug, find_duplicates

logger = logging.getLogger('nacsos_data.util.academic.import')


async def import_academic_items(
        items: list[AcademicItemModel] | Generator[AcademicItemModel, None, None],
        project_id: str | uuid.UUID,
        db_engine: DatabaseEngineAsync,
        import_name: str | None = None,
        import_id: str | uuid.UUID | None = None,
        user_id: str | uuid.UUID | None = None,
        description: str | None = None,
        check_title_slug: bool = True,
        dry_run: bool = True,
) -> None:
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


    :param items: A list (or generator) of AcademicItems
    :param project_id: ID of the project the items should be added to
    :param import_id: (optional) ID to existing Import
    :param user_id: (your) user_id, which this import will be associated with
    :param import_name: Concise and descriptive name for this import
    :param description: Proper (markdown) description for this import.
                        Usually this should describe the source of the dataset and, if applicable, the search query.
    :param check_title_slug: If true, use title_slug for duplicate detection
    :param dry_run: If false, actually write data to the database;
                    If true, simulate best as possible (note, that duplicates within the `items` are not validated
                                                        and not all constraints can be checked)
    :param db_engine: an async database engine
    :return:
    """
    if project_id is None:
        raise AttributeError('You have to provide a project ID!')

    if items is None:
        raise AttributeError('You have to provide data!')

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

            # ensure we have a title_slug
            if item.title_slug is None or len(item.title_slug) == 0:
                item.title_slug = str_to_title_slug(item.title)

            duplicates = await find_duplicates(item=item,
                                               project_id=str(project_id),
                                               check_tslug=check_title_slug,
                                               check_doi=True,
                                               check_wos_id=True,
                                               check_scopus_id=True,
                                               check_oa_id=True,
                                               check_pubmed_id=True,
                                               check_s2_id=True,
                                               session=session)

            try:
                if duplicates is not None and len(duplicates) > 0:
                    item_id = duplicates[0]
                    if dry_run:
                        logger.info(f'  -> There are at least {len(duplicates)}; I will probably use {item_id}')
                    else:
                        logger.debug(f' -> Has {len(duplicates)} duplicates; using {item_id}.')
                else:
                    if dry_run:
                        logger.info('  -> I will create a new AcademicItem!')
                    else:
                        item_id = str(uuid.uuid4())
                        logger.debug(f' -> Creating new item with ID {item_id}!')
                        item.item_id = item_id
                        session.add(AcademicItem(**item.dict()))
                        await session.commit()

                if dry_run:
                    logger.info('  -> I will create an m2m entry.')
                else:
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
