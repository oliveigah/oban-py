import threading
import time
from concurrent.futures import ThreadPoolExecutor

from . import _query
from ._worker import resolve_worker
from .types import Cancel, Snooze


class Runner:
    def __init__(
        self,
        *,
        oban,
        queue: str = "default",
        limit: int = 10,
        poll_interval: float = 0.1,
    ) -> None:
        self._oban = oban
        self._queue = queue
        self._limit = limit
        self._poll_interval = poll_interval

        self._executor = ThreadPoolExecutor(
            max_workers=limit, thread_name_prefix="oban-job"
        )
        self._stop_event = threading.Event()
        self._poller = threading.Thread(
            target=self._poll_loop, name="oban-poller", daemon=True
        )

    def start(self) -> None:
        self._poller.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._poller.join()
        self._executor.shutdown(wait=True)

    def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                with self._oban.get_connection() as conn:
                    # TODO: Pass the current instance/uuid information through
                    jobs = _query.fetch_jobs(
                        conn, queue=self._queue, demand=self._limit
                    )

                    # TODO: track the id of running jobs, used to maintain a limit later
                    for job in jobs:
                        future = self._executor.submit(self._execute, self._oban, job)
                        future.add_done_callback(self._handle_execution_exception)

            except Exception:
                if self._stop_event.is_set():
                    break

            time.sleep(self._poll_interval)

    # TODO: Log something useful when this is instrumented
    def _handle_execution_exception(self, future):
        error = future.exception()

        if error:
            print(f"[error] Unhandled exception in job execution: {error!r}")

    def _execute(self, oban, job):
        worker = resolve_worker(job.worker)()

        try:
            result = worker.perform(job)
        except Exception as error:
            result = error

        with oban.get_connection() as conn:
            match result:
                case Exception() as error:
                    # TODO: Calculate backoff
                    _query.error_job(conn, job, error, 1)
                case Snooze(seconds=seconds):
                    _query.snooze_job(conn, job, seconds)
                case Cancel(reason=reason):
                    _query.cancel_job(conn, job, reason)
                case _:
                    _query.complete_job(conn, job)
