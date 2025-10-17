from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._producer import Producer
    from ._leader import Leader
    from ._query import Query


class Refresher:
    def __init__(
        self,
        *,
        query: Query,
        leader: Leader,
        producers: dict[str, Producer],
        interval: float = 15.0,
        max_age: float = 60.0,
    ) -> None:
        self._leader = leader
        self._interval = interval
        self._max_age = max_age
        self._producers = producers
        self._query = query

        self._loop_task = None

    async def start(self) -> None:
        self._loop_task = asyncio.create_task(self._loop(), name="oban-refresher")

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
                await asyncio.sleep(self._interval)

                await self._refresh()
                await self._cleanup()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _refresh(self) -> None:
        uuids = [producer._uuid for producer in self._producers.values()]

        if uuids:
            await self._query.refresh_producers(uuids)

    async def _cleanup(self) -> None:
        if not self._leader.is_leader:
            return

        await self._query.cleanup_expired_producers(self._max_age)
