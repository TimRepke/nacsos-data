import datetime
import logging
import uuid

from sqlalchemy import select, delete, func, text, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as insert_pg

from nacsos_data.db.crud import upsert_orm
from nacsos_data.db.engine import ensure_session_async, DBSession
from nacsos_data.db.schemas import Import, m2m_import_item_table, Task, Project
from nacsos_data.db.schemas.imports import ImportRevision
from nacsos_data.db.schemas.items.base import Item
from nacsos_data.models.imports import ImportModel, ImportRevisionModel, M2MImportItemType
from nacsos_data.util import elapsed_timer
from nacsos_data.util.errors import MissingIdError, ParallelImportError


@ensure_session_async
async def read_all_imports_for_project(session: DBSession, project_id: uuid.UUID | str) -> list[ImportModel]:
    stmt = select(Import).where(Import.project_id == project_id)
    result = (await session.execute(stmt)).scalars().all()
    return [ImportModel.model_validate(res.__dict__) for res in result]


@ensure_session_async
async def read_item_count_for_import(session: DBSession, import_id: uuid.UUID | str) -> int:
    stmt = select(func.count()).select_from(m2m_import_item_table).where(m2m_import_item_table.c.import_id == import_id)
    result: int | None = (await session.execute(stmt)).scalar()
    if result is None:
        raise NoResultFound('Something went majorly wrong...')
    return result


@ensure_session_async
async def read_import(session: DBSession, import_id: uuid.UUID | str) -> ImportModel | None:
    stmt = select(Import).where(Import.import_id == import_id)
    result = (await session.execute(stmt)).scalars().one_or_none()
    if result is not None:
        return ImportModel.model_validate(result.__dict__)
    return None


@ensure_session_async
async def upsert_import(session: DBSession, import_model: ImportModel) -> str | uuid.UUID | None:
    key = await upsert_orm(
        upsert_model=import_model,
        Schema=Import,
        primary_key=Import.import_id.name,
        session=session,
    )  # FIXME: does this need `use_commit=True` ?
    return key


@ensure_session_async
async def delete_import(session: DBSession, import_id: uuid.UUID | str) -> None:
    """
    When an import is deleted, we want to also delete all items that belonged to that import
    and that import only.
    """
    # Delete m2m relations
    stmt = delete(m2m_import_item_table).where(m2m_import_item_table.c.import_id == import_id)
    await session.execute(stmt)

    # Delete related tasks
    pipeline_task_ids = (
        (
            await session.execute(
                select(ImportRevision.pipeline_task_id).where(ImportRevision.import_id == import_id, ImportRevision.pipeline_task_id.is_not(None)),
            )
        )
        .scalars()
        .all()
    )
    await session.execute(delete(Task).where(Task.task_id.in_(pipeline_task_ids)))

    # TODO rm -r .tasks/user_data/{imp.config.sources}
    # TODO rm -r .tasks/artefacts/{task.task_id}

    # Delete related revisions
    await session.execute(delete(ImportRevision).where(ImportRevision.import_id == import_id))

    # Delete import
    await session.execute(delete(Import).where(Import.import_id == import_id))

    # Delete items that no longer belong to any imports (this will cascade to delete their
    # assignments and annotations, so be careful!
    await session.execute(delete(Item).where(~Item.imports.any()))

    # Send changes to database
    await session.flush_or_commit()


@ensure_session_async
async def get_or_create_import(
    session: DBSession,
    project_id: str,
    import_name: str | None = None,
    import_id: str | uuid.UUID | None = None,
    user_id: str | uuid.UUID | None = None,
    description: str | None = None,
    i_type: str = 'script',
) -> Import:
    if import_name is not None:
        if description is None or user_id is None:
            raise AttributeError('You need to provide a meaningful description and a user id!')

        if import_id is None:
            import_id = uuid.uuid4()

        import_orm: Import = Import(
            project_id=project_id,
            user_id=user_id,
            import_id=import_id,
            name=import_name,
            description=description,
            type=i_type,
            time_created=datetime.datetime.now(),
        )
        session.add(import_orm)
        await session.flush_or_commit()
        return import_orm

    if import_id is not None:
        # check that the uuid actually exists...
        import_orm = await session.get(Import, {'import_id': import_id})  # type: ignore[assignment]
        if import_orm is None:
            raise KeyError('No import found for the given ID!')
        if str(import_orm.project_id) != str(project_id):
            raise AssertionError(f'The project ID does not match with the `Import` you provided: "{import_orm.project_id}" vs "{project_id}"')
        return import_orm

    raise AttributeError('Seems like neither provided information for creating a new import nor the ID to an existing import!')


async def set_session_mutex(session: AsyncSession, project_id: str | uuid.UUID, lock: bool) -> None:
    # We assume everything relevant was committed beforehand
    await session.rollback()

    project: Project | None = (await session.execute(select(Project).where(Project.project_id == project_id))).scalar()
    if project is None:
        raise MissingIdError(f'No project for ID={project_id}!')

    # If this is not the unset call, prevent further execution
    if lock is True and project.import_mutex:
        raise ParallelImportError('You should not run parallel imports!')

    # Set or free our import mutex
    project.import_mutex = True if lock else None
    await session.commit()


async def upsert_m2m(session: AsyncSession, item_id: str, import_id: str, latest_revision: int, dry_run: bool, logger: logging.Logger) -> None:
    if dry_run:
        logger.debug(' [DRY-RUN] -> Upserted many-to-many relationship for import/item')
    else:
        stmt_m2m = (
            insert_pg(m2m_import_item_table)
            .values(item_id=item_id, import_id=import_id, type=M2MImportItemType.explicit, first_revision=latest_revision, latest_revision=latest_revision)
            .on_conflict_do_update(
                constraint='m2m_import_item_pkey',
                set_={
                    'latest_revision': latest_revision,
                },
            )
        )
        await session.execute(stmt_m2m)
        await session.flush()
        logger.debug(' -> Upserted many-to-many relationship for import/item')


async def update_revision_statistics(
    session: AsyncSession,
    import_id: str | uuid.UUID,
    revision_id: str | uuid.UUID,
    latest_revision: int,
    num_new_items: int,
    num_updated: int,
    logger: logging.Logger,
) -> None:
    with elapsed_timer(logger, 'Updating revision stats...'):
        num_items: int = (  # type: ignore[assignment]
            await session.execute(
                text('SELECT count(1) FROM m2m_import_item WHERE import_id = :import_id'),
                {'import_id': import_id},
            )
        ).scalar()
        num_items_new: int = (  # type: ignore[assignment]
            await session.execute(
                text('SELECT count(1) FROM m2m_import_item WHERE first_revision = :rev AND import_id=:import_id'),
                {'rev': latest_revision, 'import_id': import_id},
            )
        ).scalar()
        num_items_removed: int = (  # type: ignore[assignment]
            await session.execute(
                text('SELECT count(1) FROM m2m_import_item WHERE latest_revision = :rev AND import_id=:import_id'),
                {'rev': latest_revision - 1, 'import_id': import_id},
            )
        ).scalar()
        revision_stats = {
            'num_items': num_items,
            'num_items_retrieved': num_new_items,
            'num_items_new': num_items_new,
            'num_items_updated': num_updated,
            'num_items_removed': num_items_removed,
        }
        logger.info(f'Setting new revision stats: {revision_stats}')
        await session.execute(update(ImportRevision).where(ImportRevision.import_revision_id == revision_id).values(**revision_stats))
        await session.commit()

    return None


async def get_latest_revision_counter(session: AsyncSession, import_id: str | uuid.UUID) -> int:
    latest_revision: int = (
        await session.execute(  # type: ignore[assignment]
            text('SELECT COALESCE(MAX("import_revision_counter"), 0) as latest_revision FROM import_revision WHERE import_id=:import_id;'),
            {'import_id': import_id},
        )
    ).scalar()
    return latest_revision


async def get_latest_revision(session: AsyncSession, import_id: str | uuid.UUID) -> ImportRevisionModel | None:
    rslt = (
        (
            await session.execute(
                text('SELECT * FROM import_revision WHERE import_id=:import_id ORDER BY import_revision_counter DESC LIMIT 1;'),
                {'import_id': import_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if rslt:
        return ImportRevisionModel(**rslt)
    return None
