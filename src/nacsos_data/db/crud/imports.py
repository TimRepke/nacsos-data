import datetime
import uuid

from sqlalchemy import select, delete, func
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.crud import upsert_orm
from nacsos_data.db.engine import ensure_session_async
from nacsos_data.db.schemas import Import, m2m_import_item_table, Task
from nacsos_data.db.schemas.items.base import Item
from nacsos_data.models.imports import ImportModel


@ensure_session_async
async def read_all_imports_for_project(session: AsyncSession, project_id: uuid.UUID | str) -> list[ImportModel]:
    stmt = select(Import).where(Import.project_id == project_id)
    result = (await session.execute(stmt)).scalars().all()
    return [ImportModel.model_validate(res.__dict__) for res in result]


@ensure_session_async
async def read_item_count_for_import(session: AsyncSession, import_id: uuid.UUID | str) -> int:
    stmt = select(func.count()) \
        .select_from(m2m_import_item_table) \
        .where(m2m_import_item_table.c.import_id == import_id)
    result: int | None = (await session.execute(stmt)).scalar()
    if result is None:
        raise NoResultFound('Something went majorly wrong...')
    return result


@ensure_session_async
async def read_import(session: AsyncSession,
                      import_id: uuid.UUID | str) -> ImportModel | None:
    stmt = select(Import).where(Import.import_id == import_id)
    result = (await session.execute(stmt)).scalars().one_or_none()
    if result is not None:
        return ImportModel.model_validate(result.__dict__)
    return None


@ensure_session_async
async def upsert_import(session: AsyncSession, import_model: ImportModel) -> str | uuid.UUID | None:
    key = await upsert_orm(upsert_model=import_model,
                           Schema=Import,
                           primary_key=Import.import_id.name,
                           session=session)
    return key


@ensure_session_async
async def delete_import(session: AsyncSession, import_id: uuid.UUID | str) -> None:
    """
    When an import is deleted, we want to also delete all items that belonged to that import
    and that import only.
    """
    # Delete m2m relations
    stmt = delete(m2m_import_item_table).where(m2m_import_item_table.c.import_id == import_id)
    await session.execute(stmt)

    # Delete related tasks
    stmt = select(Import).where(Import.import_id == import_id)  # type: ignore[assignment]
    imp = (await session.execute(stmt)).scalars().first()
    if imp and imp.pipeline_task_id is not None:
        stmt = delete(Task).where(Task.task_id == imp.pipeline_task_id)
        await session.execute(stmt)
    # TODO rm -r .tasks/user_data/{imp.config.sources}
    # TODO rm -r .tasks/artefacts/{task.task_id}

    # Delete import
    stmt = delete(Import).where(Import.import_id == import_id)
    await session.execute(stmt)

    # Delete items that no longer belong to any imports (this will cascade to delete their
    # assignments and annotations, so be careful!
    stmt = delete(Item).where(~Item.imports.any())
    await session.execute(stmt)
    await session.commit()


@ensure_session_async
async def get_or_create_import(session: AsyncSession,
                               project_id: str,
                               import_name: str | None = None,
                               import_id: str | uuid.UUID | None = None,
                               user_id: str | uuid.UUID | None = None,
                               description: str | None = None,
                               i_type: str = 'script') -> Import:
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
            time_created=datetime.datetime.now()
        )
        session.add(import_orm)
        await session.flush()
        return import_orm

    if import_id is not None:
        # check that the uuid actually exists...
        import_orm = await session.get(Import, {'import_id': import_id})  # type: ignore[assignment]
        if import_orm is None:
            raise KeyError('No import found for the given ID!')
        if str(import_orm.project_id) != str(project_id):
            raise AssertionError(f'The project ID does not match with the `Import` you provided: '
                                 f'"{import_orm.project_id}" vs "{project_id}"')
        return import_orm

    raise AttributeError('Seems like neither provided information for creating '
                         'a new import nor the ID to an existing import!')
