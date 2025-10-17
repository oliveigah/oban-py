from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._leader import Leader
    from ._query import Query


class Pruner:
    """Manages periodic deletion of completed, cancelled, and discarded jobs.

    This class is managed internally by Oban and shouldn't be constructed directly.
    Instead, configure pruning via the Oban constructor:

        >>> async with Oban(
        ...     conn=conn,
        ...     queues={"default": 10},
        ...     pruner={"max_age": 86_400, "interval": 60.0, "limit": 20_000}
        ... ) as oban:
        ...     # Pruner runs automatically in the background
    """

    def __init__(
        self,
        *,
        query: Query,
        leader: Leader,
        max_age: int = 86_400,
        interval: float = 60.0,
        limit: int = 20_000,
    ) -> None:
        self._leader = leader
        self._max_age = max_age
        self._interval = interval
        self._limit = limit
        self._query = query

        self._loop_task = None

    async def start(self) -> None:
        self._loop_task = asyncio.create_task(self._loop(), name="oban-pruner")

    async def stop(self) -> None:
        self._loop_task.cancel()

        try:
            await self._loop_task
        except asyncio.CancelledError:
            pass

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._interval)

                if self._leader.is_leader:
                    await self._prune()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _prune(self) -> None:
        await self._query.prune_jobs(self._max_age, self._limit)
