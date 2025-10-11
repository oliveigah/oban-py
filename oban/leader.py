from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._query import Query


class Leader:
    """Manages leadership election and coordination across Oban nodes.

    This class is managed internally by Oban. Users should not construct Leader instances
    directly. Instead, check leadership status via the Oban.is_leader property:

        >>> async with Oban(conn=conn, queues={"default": 10}) as oban:
        ...     if oban.is_leader:
        ...         # Perform leader-only operations
    """

    def __init__(
        self,
        *,
        interval: float = 30.0,
        name: str = "oban",
        node: str,
        query: Query,
    ) -> None:
        """Initialize a Leader instance.

        Args:
            interval: Election interval in seconds (default: 30.0)
            name: Leadership name/key (default: "oban")
            node: Node identifier for this instance
            query: Query instance for database operations
        """
        self._interval = interval
        self._name = name
        self._node = node
        self._query = query

        self._is_leader = False
        self._loop_task = None
        self._started = asyncio.Event()

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    async def start(self) -> None:
        self._loop_task = asyncio.create_task(self._loop(), name="oban-leader")

        await self._started.wait()

    async def stop(self) -> None:
        if self._loop_task:
            self._loop_task.cancel()

            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass

        if self._is_leader:
            await self._query.resign_leader(self._name, self._node)

    async def _loop(self) -> None:
        while True:
            try:
                self._is_leader = await self._attempt_election()
            except asyncio.CancelledError:
                break
            except Exception as error:
                print(error)
                pass
            finally:
                if not self._started.is_set():
                    self._started.set()

            # Sleep for half interval if leader (to boost their refresh interval and allow them to
            # retain leadership), full interval otherwise
            sleep_duration = self._interval / 2 if self._is_leader else self._interval

            await asyncio.sleep(sleep_duration)

    async def _attempt_election(self) -> bool:
        return await self._query.attempt_leadership(
            self._name, self._node, int(self._interval), self._is_leader
        )
