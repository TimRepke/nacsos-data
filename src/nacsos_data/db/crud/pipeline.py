import datetime
import uuid
from typing import Any

from pydantic import BaseModel
from sqlalchemy import func, select, asc, desc, delete, update
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.crud import MissingIdError
from nacsos_data.db.engine import ensure_session_async
from nacsos_data.db.schemas import Task
from nacsos_data.models.pipeline import TaskStatus, TaskModel


@ensure_session_async
async def query_tasks(session: AsyncSession,
                      order_by_fields: list[tuple[str, bool]] | None = None,
                      **kwargs: Any) -> list[TaskModel] | None:
    """
    Get tasks, all of them or filtered by custom criteria specified as keyword arguments.

    :param session: session
    :param order_by_fields:
        List of tuples, where first entry is name of a column, the second a boolean (true = asc, false = desc)
    :param kwargs:
        Each entry in the kwargs dict is assumed to be an `equals ( = )` comparison.
    """
    stmt = select(Task)

    if len(kwargs) > 0:
        for key, value in kwargs.items():
            try:
                stmt = stmt.where(getattr(Task, key) == value)
            except AttributeError:
                # not a valid field in `Task`
                pass
    if order_by_fields is not None and len(order_by_fields) > 0:
        for field, direction in order_by_fields:
            try:
                if direction:
                    stmt = stmt.order_by(asc(getattr(Task, field)))
                else:
                    stmt = stmt.order_by(desc(getattr(Task, field)))
            except AttributeError:
                # not a valid field in `Task`
                pass
    else:
        stmt = stmt.order_by(desc(Task.time_created))

    result = (await session.execute(stmt)).scalars().all()
    if len(result) == 0:
        return None
    return [TaskModel.model_validate(r.__dict__) for r in result]


@ensure_session_async
async def read_task_by_id(session: AsyncSession, task_id: str | uuid.UUID) -> TaskModel | None:
    stmt = select(Task).where(Task.task_id == task_id)
    result = (await session.execute(stmt)).scalars().one_or_none()
    if result is not None:
        return TaskModel.model_validate(result.__dict__)
    return None


@ensure_session_async
async def read_tasks_by_ids(session: AsyncSession, task_ids: list[str] | list[uuid.UUID]) -> list[TaskModel]:
    stmt = select(Task).where(Task.task_id.in_(task_ids))
    return [TaskModel.model_validate(r.__dict__)
            for r in (await session.execute(stmt)).scalars().all()]


@ensure_session_async
async def read_tasks_by_fingerprint(session: AsyncSession, fingerprint: str) -> list[TaskModel]:
    stmt = select(Task).where(Task.fingerprint == fingerprint)
    return [TaskModel.model_validate(r.__dict__)
            for r in (await session.execute(stmt)).scalars().all()]


@ensure_session_async
async def read_num_tasks_for_fingerprint(session: AsyncSession, fingerprint: str) -> int:
    stmt = select(func.count(Task.task_id)).where(Task.fingerprint == fingerprint)
    result = await session.execute(stmt)
    return result.scalar() or 0


@ensure_session_async
async def check_fingerprint_exists(session: AsyncSession, fingerprint: str) -> bool:
    return await read_num_tasks_for_fingerprint(fingerprint=fingerprint, session=session) > 0


@ensure_session_async
async def check_task_id_exists(session: AsyncSession, task_id: str | uuid.UUID) -> bool:
    stmt = select(func.count(Task.task_id)).where(Task.task_id == task_id)
    result = await session.execute(stmt)
    return (result.scalar() or 0) > 0


class StatusForTask(BaseModel):
    task_id: str | uuid.UUID
    status: TaskStatus | str


@ensure_session_async
async def read_task_statuses(session: AsyncSession, task_ids: list[str] | list[uuid.UUID]) -> list[StatusForTask]:
    stmt = select(Task.task_id, Task.status).where(Task.task_id.in_(task_ids))
    return [StatusForTask.model_validate(r.__dict__)
            for r in (await session.execute(stmt)).scalars().all()]


@ensure_session_async
async def read_task_status_by_id(session: AsyncSession, task_id: str | uuid.UUID) -> TaskStatus | str | None:
    stmt = select(Task.status).where(Task.task_id == task_id)
    result: TaskStatus | str | None = await session.scalar(stmt)
    return result


@ensure_session_async
async def delete_task_by_id(session: AsyncSession, task_id: str | uuid.UUID) -> None:
    stmt = delete(Task).where(Task.task_id == task_id)
    await session.execute(stmt)


@ensure_session_async
async def delete_tasks_by_fingerprint(session: AsyncSession, fingerprint: str) -> None:
    stmt = delete(Task).where(Task.fingerprint == fingerprint)
    await session.execute(stmt)


@ensure_session_async
async def reset_failed(session: AsyncSession) -> None:
    stmt = update(Task).where(Task.status == TaskStatus.RUNNING).values(status=TaskStatus.FAILED)
    await session.execute(stmt)


@ensure_session_async
async def upsert_task(session: AsyncSession, task: TaskModel) -> TaskModel:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    result = (await session.scalars(pg_insert(Task)
                                    .values(**task.model_dump())
                                    .on_conflict_do_update(index_elements=[Task.task_id],
                                                           set_=task.model_dump())
                                    .returning(Task),
                                    execution_options={"populate_existing": True})).one_or_none()
    return TaskModel.model_validate(result.__dict__)


@ensure_session_async
async def update_status(session: AsyncSession, task_id: str | uuid.UUID, status: TaskStatus) -> None:
    task: Task | None = (await session.scalars(select(Task).where(Task.task_id == task_id))).one_or_none()
    if task is None:
        raise MissingIdError(f'No task with ID={task_id} in database!')

    task.status = status

    if status == TaskStatus.PENDING:
        task.time_finished = None
        task.time_started = None
    elif status == TaskStatus.RUNNING:
        task.time_started = datetime.datetime.now()
    elif status == TaskStatus.COMPLETED:
        task.time_finished = datetime.datetime.now()
    elif status == TaskStatus.FAILED:
        task.time_finished = datetime.datetime.now()
    elif status == TaskStatus.CANCELLED:
        task.time_finished = datetime.datetime.now()

    await session.commit()
