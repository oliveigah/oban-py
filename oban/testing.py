"""Testing helpers for Oban workers and queues.

This module provides utilities for unit testing workers without database interaction.
"""

import json

from datetime import datetime, timezone

from .job import Job
from ._worker import resolve_worker


def process_job(job: Job):
    """Execute a worker's process method with the given job.

    This helper is designed for unit testing workers in isolation without
    requiring database interaction.

    Args:
        job: A Job instance to process

    Returns:
        The result from the worker's process method (any value is accepted)

    Raises:
        WorkerResolutionError: If the worker cannot be resolved from the job's worker path

    Example:
        >>> from oban import worker
        >>> from oban.testing import process_job
        >>>
        >>> @worker()
        ... class EmailWorker:
        ...     def process(self, job):
        ...         return {"sent": True, "to": job.args["to"]}
        >>>
        >>> def test_email_worker():
        ...     job = EmailWorker.new({"to": "user@example.com", "subject": "Hello"})
        ...     result = process_job(job)
        ...     assert result["sent"] is True
        ...     assert result["to"] == "user@example.com"

        You can also test function-based workers using the @job decorator:

        >>> from oban import job
        >>> from oban.testing import process_job
        >>>
        >>> @job()
        ... def send_notification(user_id: int, message: str):
        ...     return f"Sent '{message}' to user {user_id}"
        >>>
        >>> def test_send_notification():
        ...     job = send_notification.new(123, "Hello World")
        ...     result = process_job(job)
        ...     assert result == "Sent 'Hello World' to user 123"
    """
    now = datetime.now(timezone.utc)

    job.args = json.loads(json.dumps(job.args))
    job.meta = json.loads(json.dumps(job.meta))

    if job.id is None:
        job.id = id(job)

    if job.attempt == 0:
        job.attempt = 1

    if job.attempted_at is None:
        job.attempted_at = now

    if job.scheduled_at is None:
        job.scheduled_at = now

    if job.inserted_at is None:
        job.inserted_at = now

    worker_cls = resolve_worker(job.worker)

    return worker_cls().process(job)
