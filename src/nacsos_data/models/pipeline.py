import json
import uuid
from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import Any
from pydantic import BaseModel


class TaskStatus(str, Enum):
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


class _BaseTask(BaseModel):
    # unique identifier for this task
    task_id: str | uuid.UUID | None = None
    # name of the function (including module path)
    function_name: str
    # user_id (from nacsos-core) who triggered this task
    user_id: str | uuid.UUID | None = None
    # project_id (from nacsos-core) as context where task was triggered
    project_id: str | uuid.UUID | None = None
    # celery task id
    celery_id: str | uuid.UUID | None = None
    # user comment to keep notes
    comment: str | None = None


class BaseTask(_BaseTask):
    # json-encoded dict of the call parameters (or the dict unpacked)
    params: dict[str, Any] | str | None = None


class TaskModel(BaseTask):
    # a unique hash value (based on function name and parameters)
    fingerprint: str
    # datetime YYYY-MM-DDThh:mm:ss of when task was submitted, started, and finished
    time_created: datetime | None = None
    time_started: datetime | None = None
    time_finished: datetime | None = None

    # recommended time to schedule cleanup (e.g. deletion) of artefacts
    # leave `None` to never schedule a cleanup
    rec_expunge: datetime | None = None

    # current status of the task
    status: TaskStatus = TaskStatus.PENDING


def compute_fingerprint(full_name: str, params: dict[str, Any] | str | None) -> str:
    obj = {
        'func': full_name,
        'params': params
    }
    fingerprint = json.dumps(obj)
    fingerprint_hash = sha256(fingerprint.encode('utf-8'))
    return fingerprint_hash.hexdigest()
