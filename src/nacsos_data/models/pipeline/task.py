import uuid
from datetime import datetime
from typing import Any
from pydantic import BaseModel

from .artefact import SerializedArtefactReference
from .enums import TaskStatus, CPULoadClassification, ExecutionLocation
from .lib import APIParameters


class _BaseTask(BaseModel):
    # unique identifier for this task
    task_id: str | uuid.UUID | None = None
    # name of the function (including module path)
    function_name: str
    # user_id (from nacsos-core) who triggered this task
    user_id: str | uuid.UUID | None = None
    # project_id (from nacsos-core) as context where task was triggered
    project_id: str | uuid.UUID | None = None
    # user comment to keep notes
    comment: str | None = None
    # where this task is running
    location: ExecutionLocation = ExecutionLocation.LOCAL


class BaseTask(_BaseTask):
    # json-encoded dict of the call parameters (or the dict unpacked)
    params: dict[str, int | float | str | dict[str, Any] | SerializedArtefactReference] | str | None = None


class SubmittedTask(BaseTask):
    # Set this to `true` if the task should be scheduled regardless of whether it was
    # previously run (based on fingerprint)
    force_run: bool = False
    # A list of additional dependencies (task_ids) that can't be derived from ArtefactReferences
    forced_dependencies: list[str] | list[uuid.UUID] | None = None


class TaskModel(BaseTask):
    # a unique hash value (based on function name and parameters)
    fingerprint: str
    # datetime YYYY-MM-DDThh:mm:ss of when task was submitted, started, and finished
    time_created: datetime | None = None
    time_started: datetime | None = None
    time_finished: datetime | None = None

    # estimated runtime (in seconds) and memory (in megabyte) consumption
    # Leave `None` if unknown (not encouraged) or relatively short/constant runtime
    est_runtime: float | None = None
    est_memory: float | None = None
    est_cpu_load: CPULoadClassification = CPULoadClassification.MEDIUM

    # recommended time to schedule cleanup (e.g. deletion) of artefacts
    # leave `None` to never schedule a cleanup
    rec_expunge: datetime | None = None

    # indicates the tasks (referenced by task_id) this task depends on (or None if no dependencies exist)
    dependencies: list[str] | list[uuid.UUID] | None = None
    # current status of the task
    status: TaskStatus = TaskStatus.PENDING


class TaskPayload(BaseModel):
    import_id: str | uuid.UUID
    user_id: str | uuid.UUID
    project_id: str | uuid.UUID
    name: str
    params: APIParameters

    @property
    def payload(self) -> dict[str, Any]:
        return {
            'task_id': None,
            'function_name': self.params.func_name,
            'params': self.params.payload,
            'user_id': str(self.user_id),
            'project_id': str(self.project_id),
            'comment': f'Import for "{self.name}" ({self.import_id})',
            'location': 'LOCAL',
            'force_run': True,
            'forced_dependencies': None,
        }
