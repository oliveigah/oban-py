from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._notifier import Notifier
    from ._query import Query


class Leader:
    """Manages leadership election and coordination across Oban nodes.

    This class is managed internally by Oban and shouldn't be constructed directly.
    Instead, check leadership status via the Oban.is_leader property:

        >>> async with Oban(conn=conn, queues={"default": 10}) as oban:
        ...     if oban.is_leader:
        ...         # Perform leader-only operations
    """

    def __init__(
        self,
        *,
        enabled: bool = True,
        interval: float = 30.0,
        name: str = "oban",
        node: str,
        notifier: Notifier,
        query: Query,
    ) -> None:
        self._enabled = enabled
        self._interval = interval
        self._name = name
        self._node = node
        self._notifier = notifier
        self._query = query

        self._is_leader = False
        self._listen_token = None
        self._loop_task = None
        self._started = asyncio.Event()

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    async def start(self) -> None:
        if not self._enabled:
            self._started.set()
            return

        self._listen_token = await self._notifier.listen(
            "leader", self._on_leader_notification, wait=False
        )
        self._loop_task = asyncio.create_task(self._loop(), name="oban-leader")

        await self._started.wait()

    async def stop(self) -> None:
        if self._listen_token:
            await self._notifier.unlisten(self._listen_token)

        if self._loop_task:
            self._loop_task.cancel()

            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass

        if self._is_leader:
            payload = {"action": "resign", "node": self._node, "name": self._name}

            await self._notifier.notify("leader", payload)
            await self._query.resign_leader(self._name, self._node)

    async def _loop(self) -> None:
        while True:
            try:
                await self._attempt_election()
            except asyncio.CancelledError:
                break
            except Exception:
                pass
            finally:
                if not self._started.is_set():
                    self._started.set()

            # Sleep for half interval if leader (to boost their refresh interval and allow them to
            # retain leadership), full interval otherwise
            sleep_duration = self._interval / 2 if self._is_leader else self._interval

            await asyncio.sleep(sleep_duration)

    async def _attempt_election(self) -> None:
        self._is_leader = await self._query.attempt_leadership(
            self._name, self._node, int(self._interval), self._is_leader
        )

    async def _on_leader_notification(self, _channel: str, _payload: dict) -> None:
        await self._attempt_election()
