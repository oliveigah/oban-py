import asyncio

import pytest

from oban import worker
from oban._metrics import Metrics, _build_gauge, _build_sketch, _compute_bin


@worker()
class MetricsTestWorker:
    async def process(self, job):
        pass


class TestMetricsValidation:
    def test_valid_config_passes(self):
        Metrics(
            name="Oban",
            node="worker.1",
            notifier=None,
            producers={},
            interval=1.0,
        )

    def test_interval_must_be_positive(self):
        with pytest.raises(ValueError, match="interval must be positive"):
            Metrics(
                name="Oban",
                node="worker.1",
                notifier=None,
                producers={},
                interval=0,
            )

        with pytest.raises(ValueError, match="interval must be positive"):
            Metrics(
                name="Oban",
                node="worker.1",
                notifier=None,
                producers={},
                interval=-1.0,
            )


class TestMetricsConfig:
    @pytest.mark.oban(queues={"alpha": 1})
    async def test_metrics_disabled_by_default(self, oban_instance):
        async with oban_instance() as oban:
            assert oban._metrics is None

    @pytest.mark.oban(queues={"alpha": 1}, metrics=True)
    async def test_metrics_enabled_with_true(self, oban_instance):
        async with oban_instance() as oban:
            assert oban._metrics is not None

    @pytest.mark.oban(queues={"alpha": 1}, metrics={"interval": 0.5})
    async def test_metrics_enabled_with_interval(self, oban_instance):
        async with oban_instance() as oban:
            assert oban._metrics is not None
            assert oban._metrics._interval == 0.5


class TestMetricsBroadcast:
    @pytest.mark.oban(queues={"alpha": 2, "gamma": 3}, metrics={"interval": 60})
    async def test_broadcasts_queue_checks_on_gossip(self, oban_instance):
        received = asyncio.Queue()

        def callback(channel, payload):
            received.put_nowait((channel, payload))

        async with oban_instance() as oban:
            await oban._notifier.listen("gossip", callback)

            # Trigger broadcast directly instead of waiting for loop
            await oban._metrics._broadcast_checks()

            channel, payload = await asyncio.wait_for(received.get(), timeout=1.0)

            assert channel == "gossip"
            assert "checks" in payload

            checks = payload["checks"]
            assert len(checks) == 2

            queues = {check["queue"] for check in checks}
            assert queues == {"alpha", "gamma"}

            for check in checks:
                assert "node" in check
                assert "name" in check
                assert "queue" in check
                assert "local_limit" in check
                assert "paused" in check
                assert "running" in check
                assert "started_at" in check

            limits = {check["queue"]: check["local_limit"] for check in checks}
            assert limits == {"alpha": 2, "gamma": 3}

    @pytest.mark.oban(queues={"default": 5}, metrics={"interval": 0.01})
    async def test_broadcasts_periodically_via_loop(self, oban_instance):
        received = asyncio.Queue()

        def callback(_channel, payload):
            received.put_nowait(payload)

        async with oban_instance() as oban:
            await oban._notifier.listen("gossip", callback)

            payload = await asyncio.wait_for(received.get(), timeout=1.0)

            assert "checks" in payload
            assert len(payload["checks"]) == 1
            assert payload["checks"][0]["queue"] == "default"
            assert payload["checks"][0]["local_limit"] == 5


class TestGaugeAndSketch:
    def test_build_gauge_sums_values(self):
        gauge = _build_gauge([1, 2, 3, 4, 5])

        assert gauge == {"data": [15]}

    def test_build_gauge_single_value(self):
        gauge = _build_gauge([42])

        assert gauge == {"data": [42]}

    def test_build_sketch_creates_bins(self):
        sketch = _build_sketch([100, 200, 300])

        assert "data" in sketch
        assert "size" in sketch
        assert sketch["size"] == 3
        assert isinstance(sketch["data"], dict)
        assert len(sketch["data"]) > 0

    def test_build_sketch_clamps_negative_values(self):
        sketch = _build_sketch([-100, -200])

        assert sketch["size"] == 2

    def test_compute_bin_is_deterministic(self):
        bin1 = _compute_bin(1000)
        bin2 = _compute_bin(1000)

        assert bin1 == bin2

    def test_compute_bin_increases_with_value(self):
        bin_small = _compute_bin(100)
        bin_large = _compute_bin(10000)

        assert bin_large > bin_small


class TestJobMetricsBroadcast:
    @pytest.mark.oban(queues={"default": 1}, metrics={"interval": 60})
    async def test_broadcasts_job_metrics_after_execution(self, oban_instance):
        received = asyncio.Queue()

        def callback(_channel, payload):
            received.put_nowait(payload)

        async with oban_instance() as oban:
            await oban._notifier.listen("metrics", callback)

            # Enqueue and wait for job to complete
            await oban.enqueue(MetricsTestWorker.new({}))
            await asyncio.sleep(0.1)

            # Trigger metrics broadcast
            await oban._metrics._broadcast_metrics()

            payload = await asyncio.wait_for(received.get(), timeout=1.0)

            assert "metrics" in payload
            assert "name" in payload
            assert "node" in payload
            assert "time" in payload

            metrics = payload["metrics"]
            assert len(metrics) == 3  # exec_time, wait_time, exec_count

            series_names = {metric["series"] for metric in metrics}
            assert series_names == {"exec_time", "wait_time", "exec_count"}

            for metric in metrics:
                assert metric["state"] == "completed"
                assert metric["queue"] == "default"
                assert metric["worker"] == "test.metrics_test.MetricsTestWorker"
                assert "value" in metric

            # Verify exec_count uses Gauge format
            exec_count = next(met for met in metrics if met["series"] == "exec_count")
            assert "data" in exec_count["value"]
            assert exec_count["value"]["data"] == [1]

            # Verify exec_time uses Sketch format
            exec_time = next(met for met in metrics if met["series"] == "exec_time")
            assert "data" in exec_time["value"]
            assert "size" in exec_time["value"]
            assert exec_time["value"]["size"] == 1

    @pytest.mark.oban(queues={"default": 1}, metrics={"interval": 60})
    async def test_no_metrics_broadcast_when_buffer_empty(self, oban_instance):
        received = asyncio.Queue()

        def callback(_channel, payload):
            received.put_nowait(payload)

        async with oban_instance() as oban:
            await oban._notifier.listen("metrics", callback)

            # Broadcast with empty buffer should not send anything
            await oban._metrics._broadcast_metrics()

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(received.get(), timeout=0.1)
