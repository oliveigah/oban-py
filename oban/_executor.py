from __future__ import annotations


from typing import TYPE_CHECKING

from .types import Cancel, Snooze
from ._worker import resolve_worker

if TYPE_CHECKING:
    from .job import Job
    from ._query import Query


class Executor:
    def __init__(self, query: Query, safe: bool = True):
        self._query = query
        self._safe = safe

    # NOTE: This only returns the intended state, but will return something structured soon.
    async def execute(self, job: Job) -> str:
        worker = resolve_worker(job.worker)()

        try:
            result = await worker.process(job)
        except Exception as error:
            result = error

        match result:
            case Exception() as error:
                if not self._safe:
                    raise error

                backoff = self._calculate_backoff(worker, job)
                await self._query.error_job(job, error, backoff)

                if job.attempt >= job.max_attempts:
                    return "discarded"
                else:
                    return "retryable"

            case Snooze(seconds=seconds):
                await self._query.snooze_job(job, seconds)

                return "scheduled"

            case Cancel(reason=reason):
                await self._query.cancel_job(job, reason)

                return "cancelled"

            case _:
                await self._query.complete_job(job)

                return "completed"

    def _calculate_backoff(self, worker, job: Job) -> int:
        if hasattr(worker, "backoff"):
            return worker.backoff(job)
        else:
            from ._backoff import jittery_clamped

            return jittery_clamped(job.attempt, job.max_attempts)
