from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .leader import Leader
    from ._query import Query


class Pruner:
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
        if self._loop_task:
            self._loop_task.cancel()

            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        while True:
            try:
                # Pruning doesn't need to happen immediately on start, so we invert the loop to
                # sleep first and prune after the delay.
                await asyncio.sleep(self._interval)

                await self._prune()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _prune(self) -> None:
        if not self._leader.is_leader:
            return

        await self._query.prune_jobs(self._max_age, self._limit)
