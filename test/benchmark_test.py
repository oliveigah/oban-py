import asyncio
import pytest

from oban import job, worker


class TestEnqueueBenchmark:
    @pytest.mark.benchmark
    def test_enqueue_10k_jobs(self, benchmark, oban_instance):
        """Benchmark inserting 10,000 jobs into the database."""

        @worker()
        class EmptyWorker:
            async def process(self, _):
                pass

        oban = oban_instance()
        jobs = [EmptyWorker.new() for _ in range(10_000)]

        async def run():
            await oban.enqueue_many(*jobs)

        benchmark(lambda: asyncio.run(run()))

    @pytest.mark.benchmark
    @pytest.mark.oban(queues={"default": 20})
    def test_insert_and_execute_1k_jobs(self, benchmark, oban_instance):
        """Benchmark inserting and executing 1,000 jobs."""
        total = 1_000
        event = None

        @job()
        def process(index):
            if index == total:
                event.set()

        async def run():
            nonlocal event
            event = asyncio.Event()

            async with oban_instance() as oban:
                jobs = [process.new(index=idx) for idx in range(1, total + 1)]
                await oban.enqueue_many(*jobs)
                await event.wait()

        benchmark(lambda: asyncio.run(run()))
