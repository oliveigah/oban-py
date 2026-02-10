from __future__ import annotations

import asyncio
import math
import time
from collections import defaultdict
from threading import Lock
from typing import TYPE_CHECKING, Any

from . import telemetry

if TYPE_CHECKING:
    from ._notifier import Notifier
    from ._producer import Producer, QueueInfo


# DDSketch with 2% relative error constants. These match what the `Sketch` module in
# `oban_met` uses.
_ERROR = 0.02
_GAMMA = (1 + _ERROR) / (1 - _ERROR)
_INV_LOG_GAMMA = 1.0 / math.log(_GAMMA)


def _validate(*, interval: float) -> None:
    if interval <= 0:
        raise ValueError("interval must be positive")


def _build_gauge(values: list[int]) -> dict[str, Any]:
    return {"data": [sum(values)]}


def _compute_bin(value: int) -> int:
    return math.ceil(math.log(value) * _INV_LOG_GAMMA)


def _build_sketch(values: list[int]) -> dict[str, Any]:
    bins = defaultdict(int)

    for value in values:
        clamped = max(1, abs(value))
        bins[_compute_bin(clamped)] += 1

    return {"data": dict(bins.items()), "size": len(values)}


# Buffer key: (series, state, queue, worker)
BufferKey = tuple[str, str, str, str]


class Metrics:
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

        self._buffer = defaultdict(list)
        self._buffer_lock = Lock()
        self._handler_id = f"oban-metrics-{name}"
        self._loop_task = None

        _validate(interval=interval)

    async def start(self) -> None:
        telemetry.attach(
            self._handler_id,
            ["oban.job.stop", "oban.job.exception"],
            self._handle_job_event,
        )

        self._loop_task = asyncio.create_task(self._loop(), name="oban-metrics")

    async def stop(self) -> None:
        telemetry.detach(self._handler_id)

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
                await self._broadcast_metrics()
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    def _handle_job_event(self, _name: str, meta: dict[str, Any]) -> None:
        job = meta["job"]
        state = meta["state"]
        queue = job.queue
        worker = job.worker

        exec_time = meta["duration"]
        wait_time = meta["queue_time"]

        with self._buffer_lock:
            self._buffer[("exec_time", state, queue, worker)].append(exec_time)
            self._buffer[("wait_time", state, queue, worker)].append(wait_time)
            self._buffer[("exec_count", state, queue, worker)].append(1)

    async def _broadcast_checks(self) -> None:
        checks = [
            self._check_to_dict(prod.check()) for prod in self._producers.values()
        ]

        if checks:
            await self._notifier.notify("gossip", {"checks": checks})

    async def _broadcast_metrics(self) -> None:
        with self._buffer_lock:
            buffer = self._buffer
            self._buffer = defaultdict(list)

        if not buffer:
            return

        metrics = []

        for (series, state, queue, worker), values in buffer.items():
            if series == "exec_count":
                value = _build_gauge(values)
            else:
                value = _build_sketch(values)

            metrics.append(
                {
                    "series": series,
                    "state": state,
                    "queue": queue,
                    "worker": worker,
                    "value": value,
                }
            )

        await self._notifier.notify(
            "metrics",
            {
                "metrics": metrics,
                "name": self._name,
                "node": self._node,
                "time": int(time.time()),
            },
        )

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
