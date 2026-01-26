import asyncio

import pytest

from oban._metrics import Metrics


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
