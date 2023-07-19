import json
import uuid
from enum import Enum
from hashlib import sha256
from datetime import datetime
from typing import Any
from typing_extensions import TypedDict

from pydantic import BaseModel


class SerializedArtefact(TypedDict):
    """
    The SerializedArtefact is the interface definition on how references to artefacts
    are communicated. It is essentially just a proxy for `Artefact`.
    """
    serializer: str
    dtype: str
    filename: str | None
    filenames: str | list[str] | None

    # FIXME mark optional keys as NotRequired once we are on Python 3.11
    #       https://docs.python.org/3.11/library/typing.html#typing.NotRequired


class SerializedArtefactReference(TypedDict):
    task_id: str
    artefact: str


class SerializedUserArtefact(TypedDict):
    user_serializer: str
    user_dtype: str
    filename: str | None
    filenames: list[str] | None


# Can be used to indicate how CPU intensive a task usually is. Guide below
# VHIGH -> Full throttle on all available cores all the time
# HIGH -> Bursts/periods of full throttle on all cores, but intermittent
# MEDIUM -> Uses one or a few cores quite intensely
# LOW -> Uses one or a few cores sparingly
# MINIMAL -> Rather insignificant CPU needs (either because it's a very short computation or just not comp. heavy)
class CPULoadClassification(str, Enum):
    VHIGH = 'VHIGH'
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'
    MINIMAL = 'MINIMAL'


class KWARG(BaseModel):
    # list of allowed types (usually just one entry, otherwise for unions)
    dtype: list[str]
    # whether this is an optional parameter or not (then None)
    optional: bool | None = None
    # default value (None if no default is given)
    default: int | float | bool | str | None = None
    # only used if KWARG has dtype Artefact
    artefact: SerializedArtefact | None = None
    # only used if dtype is more complex and has sub-objects
    # dict of key = param name, value = datatype or tuple(datatype, default value)
    params: dict[str, 'KWARG'] | None = None
    # used for `Literal`
    options: list[str] | None = None
    # used for generics
    generics: list[str] | None = None


class TaskStatus(str, Enum):
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


class ExecutionLocation(str, Enum):
    LOCAL = 'LOCAL'
    PIK = 'PIK'


class BaseTask(BaseModel):
    # unique identifier for this task
    task_id: str | uuid.UUID | None = None
    # name of the function (including module path)
    function_name: str
    # json-encoded dict of the call parameters (or the dict unpacked)
    params: dict[str, int | float | str | dict[str, Any] | SerializedArtefactReference] | str | None = None
    # user_id (from nacsos-core) who triggered this task
    user_id: str | uuid.UUID | None = None
    # project_id (from nacsos-core) as context where task was triggered
    project_id: str | uuid.UUID | None = None
    # user comment to keep notes
    comment: str | None = None
    # where this task is running
    location: ExecutionLocation = ExecutionLocation.LOCAL


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


def compute_fingerprint(full_name: str, params: dict[str, Any] | str | None) -> str:
    obj = {
        'func': full_name,
        'params': params
    }
    fingerprint = json.dumps(obj)
    fingerprint_hash = sha256(fingerprint.encode('utf-8'))
    return fingerprint_hash.hexdigest()
