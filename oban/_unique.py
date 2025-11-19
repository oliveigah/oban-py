from __future__ import annotations

import base64
import hashlib
from dataclasses import replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import orjson

if TYPE_CHECKING:
    from .job import Job

# Kept here for reference, primarily.
STATES_TO_INTS = {
    "scheduled": 0,
    "available": 1,
    "executing": 2,
    "retryable": 3,
    "completed": 4,
    "cancelled": 5,
    "discarded": 6,
}

GROUP_TO_INTS = {
    "all": [val for (_, val) in STATES_TO_INTS.items()],
    "incomplete": [0, 1, 2, 3],
    "scheduled": [0],
    "successful": [4],
}


def with_uniq_meta(job: Job) -> Job:
    if not job.unique:
        return job

    uniq_meta = {"uniq": True, "uniq_bmp": _gen_bmp(job), "uniq_key": _gen_key(job)}

    meta = {**job.meta, **uniq_meta}

    return replace(job, meta=meta)


def _gen_bmp(job: Job) -> list[int]:
    if not job.unique:
        raise RuntimeError("Unique required")

    return GROUP_TO_INTS.get(job.unique["group"], [])


def _gen_key(job: Job) -> str:
    if not job.unique:
        raise RuntimeError("Unique required")

    fields = job.unique["fields"]
    keys = job.unique["keys"]
    period = job.unique["period"]

    data = []
    for field in sorted(fields):
        if field == "args":
            data.append(_take_keys(job.args, keys))
        elif field == "meta":
            data.append(_take_keys(job.meta, keys))
        elif field == "worker":
            data.append(job.worker)
        elif field == "queue":
            data.append(job.queue)

    if period is not None:
        timestamp = job.scheduled_at or datetime.now(timezone.utc)

        data.insert(0, truncate(period, timestamp))

    return hash64(data)


def _take_keys(data: dict[str, Any], keys: list[str] | None) -> list[tuple[str, str]]:
    if not data:
        return []

    if not keys:
        keys = list(data.keys())

    return [(key, _hash_val(data.get(key))) for key in sorted(keys)]


def _hash_val(val: Any) -> str:
    match val:
        case str():
            return val
        case int() | float() | bool():
            return str(val)
        case dict() | list():
            return orjson.dumps(val, option=orjson.OPT_SORT_KEYS).decode()
        case _:
            return str(val)


def truncate(period: int, timestamp: datetime) -> str:
    u_seconds = int(timestamp.timestamp())
    remainder = u_seconds % period
    truncated = u_seconds - remainder

    return datetime.fromtimestamp(truncated, tz=timezone.utc).isoformat()


def hash64(data: list[Any]) -> str:
    serialized = orjson.dumps(data, option=orjson.OPT_SORT_KEYS)
    hashed = hashlib.blake2s(serialized)

    return base64.b64encode(hashed.digest()).decode().rstrip("=")
