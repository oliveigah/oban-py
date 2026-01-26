from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._notifier import Notifier
    from ._producer import Producer, QueueInfo


def _validate(*, interval: float) -> None:
    if interval <= 0:
        raise ValueError("interval must be positive")


class Metrics:
    """Broadcasts queue state for Oban Web integration.

    Publishes queue checks on the gossip channel, including local producer state
    such as limits, paused status, and running jobs. Published by all nodes.

    This looper is disabled by default to avoid pub/sub overhead. Enable it by
    passing `metrics=True` or `metrics={"interval": 0.5}` to Oban.
    """

    def __init__(
        self,
        *,
        name: str,
        node: str,
        notifier: Notifier,
        producers: dict[str, Producer],
        interval: float = 1.0,
    ) -> None:
        self._name = name
        self._node = node
        self._notifier = notifier
        self._producers = producers
        self._interval = interval

        self._loop_task = None

        _validate(interval=interval)

    async def start(self) -> None:
        self._loop_task = asyncio.create_task(self._loop(), name="oban-metrics")

    async def stop(self) -> None:
        if not self._loop_task:
            return

        self._loop_task.cancel()

        try:
            await self._loop_task
        except asyncio.CancelledError:
            pass

    async def _loop(self) -> None:
        while True:
            try:
                await self._broadcast_checks()
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _broadcast_checks(self) -> None:
        checks = [self._check_to_dict(prod.check()) for prod in self._producers.values()]

        if checks:
            await self._notifier.notify("gossip", {"checks": checks})

    def _check_to_dict(self, check: QueueInfo) -> dict[str, Any]:
        started_at = check.started_at.isoformat() if check.started_at else None

        return {
            "node": check.node,
            "name": self._name,
            "queue": check.queue,
            "local_limit": check.limit,
            "paused": check.paused,
            "running": check.running,
            "started_at": started_at,
        }
