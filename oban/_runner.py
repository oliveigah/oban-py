from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from . import _query
from ._backoff import jittery_clamped
from ._worker import resolve_worker
from .job import Job
from .types import Cancel, Snooze

if TYPE_CHECKING:
    from .oban import Oban


class Runner:
    def __init__(
        self,
        *,
        oban: Oban,
        queue: str = "default",
        limit: int = 10,
        uuid: str,
    ) -> None:
        self._oban = oban
        self._queue = queue
        self._limit = limit
        self._uuid = uuid

        self._executor = ThreadPoolExecutor(
            max_workers=limit, thread_name_prefix=f"oban-{queue}"
        )
        self._stop_event = threading.Event()
        self._work_available = threading.Event()
        self._poller = threading.Thread(
            target=self._loop, name=f"oban-runner-{queue}", daemon=True
        )

    def start(self) -> None:
        self._poller.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._work_available.set()  # Wake up the thread
        self._poller.join()
        self._executor.shutdown(wait=True)

    def notify(self) -> None:
        """Called by stager when work is available for this queue."""
        self._work_available.set()

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            # TODO: Shorten this timeout based on configuration, the timeout changes whether we
            # cleanly break on a stop event
            if not self._work_available.wait(timeout=1.0):
                continue

            self._work_available.clear()

            if self._stop_event.is_set():
                break

            try:
                # TODO: track the id of running jobs, used to maintain a limit later
                jobs = self._fetch_jobs()

                for job in jobs:
                    future = self._executor.submit(self._execute, self._oban, job)
                    future.add_done_callback(self._handle_execution_exception)

            except Exception:
                if self._stop_event.is_set():
                    break

    def _fetch_jobs(self):
        with self._oban.get_connection() as conn:
            return _query.fetch_jobs(
                conn,
                queue=self._queue,
                demand=self._limit,
                node=self._oban._node,
                uuid=self._uuid,
            )

    # TODO: Log something useful when this is instrumented
    def _handle_execution_exception(self, future: Future[None]) -> None:
        error = future.exception()

        if error:
            print(f"[error] Unhandled exception in job execution: {error!r}")

    def _execute(self, oban: Oban, job: Job) -> None:
        worker = resolve_worker(job.worker)()

        try:
            result = worker.perform(job)
        except Exception as error:
            result = error

        with oban.get_connection() as conn:
            match result:
                case Exception() as error:
                    backoff = self._backoff(worker, job)

                    _query.error_job(conn, job, error, backoff)
                case Snooze(seconds=seconds):
                    _query.snooze_job(conn, job, seconds)
                case Cancel(reason=reason):
                    _query.cancel_job(conn, job, reason)
                case _:
                    _query.complete_job(conn, job)

    def _backoff(self, worker: Any, job: Job) -> int:
        if hasattr(worker, "backoff"):
            return worker.backoff(job)
        else:
            return jittery_clamped(job.attempt, job.max_attempts)
