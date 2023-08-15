import json
from hashlib import sha256
from typing import Any


def compute_fingerprint(full_name: str, params: dict[str, Any] | str | None) -> str:
    obj = {
        'func': full_name,
        'params': params
    }
    fingerprint = json.dumps(obj)
    fingerprint_hash = sha256(fingerprint.encode('utf-8'))
    return fingerprint_hash.hexdigest()
