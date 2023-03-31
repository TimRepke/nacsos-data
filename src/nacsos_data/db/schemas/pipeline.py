import uuid

from sqlalchemy import String, Integer, ForeignKey, DateTime, func, Enum, ARRAY
from sqlalchemy.orm import mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy_json import mutable_json_type

from ..base_class import Base
from .users import User
from .projects import Project
from ...models.pipeline import CPULoadClassification, TaskStatus, ExecutionLocation


class Task(Base):
    """
    User represents a person.
    Most entries in the database will be (indirectly) linked to user accounts, so this is
    at the core of access management and ownership.
    """
    __tablename__ = 'tasks'

    # Unique identifier for this task.
    task_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                            nullable=False, unique=True, index=True)

    # User who created this task (may be NULL if done via a script)
    user_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(User.user_id),
                            nullable=True, index=True, primary_key=False)

    # Project this task is attached to
    project_id = mapped_column(UUID(as_uuid=True),
                               ForeignKey(Project.project_id, ondelete='CASCADE'),
                               nullable=False, index=True, primary_key=False)

    # fingerprint based on the parameters for this task
    fingerprint = mapped_column(String, nullable=False, unique=False, index=True)

    # name of the pipeline function (incl full package path)
    function_name = mapped_column(String, nullable=False, unique=False, index=True)

    # indicates the tasks (referenced by task_id) this task depends on (or None if no dependencies exist)
    dependencies = mapped_column(ARRAY(UUID(as_uuid=True)), nullable=True)

    # current status of the task
    status = mapped_column(Enum(TaskStatus), nullable=False,
                           server_default=TaskStatus.PENDING)

    # where this task is running
    location = mapped_column(Enum(ExecutionLocation), nullable=False,
                             server_default=ExecutionLocation.LOCAL)

    # json-encoded dict of the call parameters (or the dict unpacked)
    params = mapped_column(mutable_json_type(dbtype=JSONB, nested=True))

    # (optional) short comment to keep notes on this task
    comment = mapped_column(String, nullable=True, unique=False, index=False)

    # Date and time when this task was created and when the actual task was triggered and finished
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now())
    time_started = mapped_column(DateTime(timezone=True), nullable=True)
    time_finished = mapped_column(DateTime(timezone=True), nullable=True)

    # (optional) estimated runtime (in seconds) for this task
    est_runtime = mapped_column(Integer, nullable=True, unique=False, index=False)
    # (optional) estimated memory (in bytes) for this task
    est_memory = mapped_column(Integer, nullable=True, unique=False, index=False)
    # (optional) estimated load on CPU for this task
    est_cpu_load = mapped_column(Enum(CPULoadClassification), nullable=False,
                                 server_default=CPULoadClassification.MEDIUM)

    # (optional) recommended time to schedule cleanup (e.g. deletion) of artefacts
    # leave `None` to never schedule a cleanup
    rec_expunge = mapped_column(DateTime(timezone=True), nullable=True)
