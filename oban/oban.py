from __future__ import annotations

import socket

from typing import Any
from uuid import uuid4

from .job import Job
from .leader import Leader
from ._producer import Producer
from ._query import Query
from ._stager import Stager

_instances: dict[str, Oban] = {}


class Oban:
    def __init__(
        self,
        *,
        conn: Any,
        leadership: bool | None = None,
        name: str = "oban",
        node: str | None = None,
        prefix: str = "public",
        queues: dict[str, int] | None = None,
        stage_interval: float = 1.0,
    ) -> None:
        """Initialize an Oban instance.

        Oban can run in two modes:
        - Server mode: When queues are configured, this instance processes jobs.
          Leadership is enabled by default to coordinate cluster-wide operations.
        - Client mode: When no queues are configured, this instance only enqueues jobs.
          Leadership is disabled by default.

        Args:
            conn: Database connection or pool (e.g., AsyncConnection or AsyncConnectionPool)
            leadership: Enable leadership election (default: True if queues configured, False otherwise)
            name: Name for this instance in the registry (default: "oban")
            node: Node identifier for this instance (default: socket.gethostname())
            prefix: PostgreSQL schema where Oban tables are located (default: "public")
            queues: Queue names mapped to worker limits (default: {})
            stage_interval: How often to stage scheduled jobs, in seconds (default: 1.0)
        """
        queues = queues or {}

        if leadership is None:
            leadership = bool(queues)

        for queue, limit in queues.items():
            if limit < 1:
                raise ValueError(f"Queue '{queue}' limit must be positive")

        if stage_interval <= 0:
            raise ValueError("stage_interval must be positive")

        self._name = name
        self._node = node or socket.gethostname()
        self._query = Query(conn, prefix)

        self._producers = {
            queue: Producer(
                query=self._query,
                node=self._node,
                queue=queue,
                limit=limit,
                uuid=str(uuid4()),
            )
            for queue, limit in queues.items()
        }

        self._stager = Stager(
            query=self._query,
            producers=self._producers,
            stage_interval=stage_interval,
        )

        self._leader = (
            Leader(query=self._query, node=self._node, name=name)
            if leadership
            else None
        )

        _instances[name] = self

    async def __aenter__(self) -> Oban:
        return await self.start()

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        await self.stop()

    @property
    def is_leader(self) -> bool:
        """Check if this node is currently the leader.

        Returns False if leadership is not enabled for this instance. Otherwise, it indicates
        whether this instance is acting as leader.

        Example:
            >>> async with Oban(conn=conn, leadership=true) as conn:
            ...     if oban.is_leader:
            ...         # Perform leader-only operation
        """
        return self._leader.is_leader if self._leader else False

    async def _verify_structure(self) -> None:
        existing = await self._query.verify_structure()

        for table in ["oban_jobs", "oban_leaders"]:
            if table not in existing:
                raise RuntimeError(
                    f"The '{table}' is missing, run schema installation first."
                )

    async def start(self) -> Oban:
        if self._producers:
            await self._verify_structure()

        for queue, producer in self._producers.items():
            await producer.start()

        if self._leader:
            await self._leader.start()

        await self._stager.start()

        return self

    async def stop(self) -> None:
        await self._stager.stop()

        for producer in self._producers.values():
            await producer.stop()

        if self._leader:
            await self._leader.stop()

    async def enqueue(self, job: Job) -> Job:
        """Insert a job into the database for processing.

        Args:
            job: A Job instance created via Worker.new()

        Returns:
            The inserted job with database-assigned values (id, timestamps, state)

        Example:
            >>> from myapp.oban import oban, EmailWorker
            >>>
            >>> job = EmailWorker.new({"to": "user@example.com", "subject": "Welcome"})
            >>> await oban.enqueue(job)

        Note:
            For convenience, you can also use Worker.enqueue() directly:

            >>> await EmailWorker.enqueue({"to": "user@example.com", "subject": "Welcome"})
        """
        result = await self._query.insert_jobs([job])

        return result[0]

    async def enqueue_many(self, *jobs: Job) -> list[Job]:
        """Insert multiple jobs into the database in a single operation.

        This is more efficient than calling enqueue() multiple times as it uses a
        single database query to insert all jobs.

        Args:
            *jobs: Job instances created via Worker.new()

        Returns:
            The inserted jobs with database-assigned values (id, timestamps, state)

        Example:
            >>> from myapp.oban import oban, EmailWorker
            >>>
            >>> job1 = EmailWorker.new({"to": "user1@example.com"})
            >>> job2 = EmailWorker.new({"to": "user2@example.com"})
            >>> job3 = EmailWorker.new({"to": "user3@example.com"})
            >>>
            >>> await oban.enqueue_many(job1, job2, job3)
        """
        return await self._query.insert_jobs(list(jobs))


def get_instance(name: str = "oban") -> Oban:
    """Get an Oban instance from the registry by name.

    Args:
        name: Name of the instance to retrieve (default: "oban")

    Returns:
        The Oban instance

    Raises:
        RuntimeError: If no instance with the given name exists
    """
    instance = _instances.get(name)

    if instance is None:
        raise RuntimeError(f"Oban instance '{name}' not found in registry")

    return instance
