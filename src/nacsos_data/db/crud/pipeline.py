import datetime
import json
import uuid
from typing import Any

from pydantic import BaseModel
from sqlalchemy import func, select, asc, desc, delete, update
from sqlalchemy.orm import Session

from nacsos_data.db import DatabaseEngine
from nacsos_data.db.crud import MissingIdError
from nacsos_data.db.schemas import Task
from nacsos_data.models.pipeline import TaskStatus, TaskModel


def query_tasks(engine: DatabaseEngine,
                order_by_fields: list[tuple[str, bool]] | None = None,
                **kwargs: Any) -> list[TaskModel] | None:
    """
    Get tasks, all of them or filtered by custom criteria specified as keyword arguments.

    :param engine: engine
    :param order_by_fields:
        List of tuples, where first entry is name of a column, the second a boolean (true = asc, false = desc)
    :param kwargs:
        Each entry in the kwargs dict is assumed to be an `equals ( = )` comparison.
    """
    with engine.session() as session:  # type: Session
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

        result = session.execute(stmt).scalars().all()
        if len(result) == 0:
            return None
        return [TaskModel.parse_obj(r.__dict__) for r in result]


def read_task_by_id(task_id: uuid.UUID | str, engine: DatabaseEngine) -> TaskModel | None:
    with engine.session() as session:  # type: Session
        stmt = select(Task).where(Task.task_id == task_id)
        result = session.execute(stmt).scalars().one_or_none()
        if result is not None:
            return TaskModel.parse_obj(result.__dict__)
        return None


def read_tasks_by_ids(task_ids: list[str], engine: DatabaseEngine) -> list[TaskModel]:
    with engine.session() as session:  # type: Session
        stmt = select(Task).where(Task.task_id.in_(task_ids))
        return [TaskModel.parse_obj(r.__dict__)
                for r in session.execute(stmt).scalars().all()]


def read_tasks_by_fingerprint(fingerprint: str, engine: DatabaseEngine) -> list[TaskModel]:
    with engine.session() as session:  # type: Session
        stmt = select(Task).where(Task.fingerprint == fingerprint)
        return [TaskModel.parse_obj(r.__dict__)
                for r in session.execute(stmt).scalars().all()]


def read_num_tasks_for_fingerprint(fingerprint: str, engine: DatabaseEngine) -> int:
    with engine.session() as session:  # type: Session
        stmt = select(func.count(Task.task_id)).where(Task.fingerprint == fingerprint)
        result = session.execute(stmt)
        return result.scalar()


def check_fingerprint_exists(fingerprint: str, engine: DatabaseEngine) -> bool:
    return read_num_tasks_for_fingerprint(fingerprint, engine) > 0


def check_task_id_exists(task_id: str, engine: DatabaseEngine) -> bool:
    with engine.session() as session:  # type: Session
        stmt = select(func.count(Task.task_id)).where(Task.task_id == task_id)
        result = session.execute(stmt)
        return (result.scalar()) > 0


class StatusForTask(BaseModel):
    task_id: str | uuid.UUID
    status: TaskStatus | str


def read_task_statuses(task_ids: list[str], engine: DatabaseEngine) -> list[StatusForTask]:
    with engine.session() as session:  # type: Session
        stmt = select(Task.task_id, Task.status).where(Task.task_id.in_(task_ids))
        return [StatusForTask.parse_obj(r.__dict__)
                for r in session.execute(stmt).scalars().all()]


def read_task_status_by_id(task_id: str, engine: DatabaseEngine) -> TaskStatus | str:
    with engine.session() as session:  # type: Session
        stmt = select(Task.status).where(Task.task_id == task_id)
        return session.execute(stmt).scalar().one_or_none()


def delete_task_by_id(task_id: uuid.UUID | str, engine: DatabaseEngine):
    with engine.session() as session:  # type: Session
        stmt = delete(Task).where(Task.task_id == task_id)
        session.execute(stmt)


def delete_tasks_by_fingerprint(fingerprint: str, engine: DatabaseEngine):
    with engine.session() as session:  # type: Session
        stmt = delete(Task).where(Task.fingerprint == fingerprint)
        session.execute(stmt)


def reset_failed(engine: DatabaseEngine):
    with engine.session() as session:  # type: Session
        stmt = update(Task).where(Task.status == TaskStatus.RUNNING).values(status=TaskStatus.FAILED)
        session.execute(stmt)


def upsert_task(task: TaskModel, engine: DatabaseEngine) -> TaskModel:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    with engine.session() as session:  # type: Session
        result = session.scalars(pg_insert(Task)
                                 .values(**task.dict())
                                 .on_conflict_do_update(index_elements=[Task.task_id],
                                                        set_=task.dict())
                                 .returning(Task),
                                 execution_options={"populate_existing": True}).one_or_none()
        print(result)
        print(result.__dict__)
        return TaskModel.parse_obj(result.__dict__)


def update_status(task_id: str | uuid.UUID, status: TaskStatus, engine: DatabaseEngine):
    with engine.session() as session:  # type: Session
        task: Task = session.scalars(select(Task).where(Task.task_id == task_id)).one_or_none()
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

        session.commit()
