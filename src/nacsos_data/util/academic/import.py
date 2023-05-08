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
                import_orm: Import | None = Import(
                    project_id=project_id,
                    user_id=user_id,
                    import_id=import_id,
                    name=import_name,
                    description=description,
                    type=ImportType.script
                )
                if dry_run:
                    logger.info('I will create a new `Import`!')
                else:
                    session.add(import_orm)
                    await session.commit()
                    logger.info(f'Created new import with ID {import_id}')

        elif import_id is not None:
            # check that the uuid actually exists...
            import_orm = await session.get(Import, {'import_id': import_id})
            if import_orm is None:
                raise KeyError('No import found for the given ID!')

            logger.info(f'Using existing import with ID {import_id}')

        else:
            raise AttributeError('Seems like neither provided information for creating '
                                 'a new import nor the ID to an existing import!')

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
