import json
from hashlib import sha256
from typing import Any

from .task import SubmittedTask, TaskPayload, TaskModel, BaseTask
from .artefact import KWARG, SerializedArtefactReference, SerializedArtefact, SerializedUserArtefact
from .enums import CPULoadClassification, TaskStatus, ExecutionLocation


def compute_fingerprint(full_name: str, params: dict[str, Any] | str | None) -> str:
    obj = {
        'func': full_name,
        'params': params
    }
    fingerprint = json.dumps(obj)
    fingerprint_hash = sha256(fingerprint.encode('utf-8'))
    return fingerprint_hash.hexdigest()


__all__ = ['compute_fingerprint',
           'SubmittedTask', 'TaskPayload', 'TaskModel', 'BaseTask',
           'KWARG', 'SerializedArtefactReference', 'SerializedArtefact', 'SerializedUserArtefact',
           'CPULoadClassification', 'TaskStatus', 'ExecutionLocation']
