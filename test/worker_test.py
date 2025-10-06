import pytest

import oban
from oban.job import Job


class TestWorkerDecorator:
    @pytest.fixture
    def basic_worker(self):
        @oban.worker(queue="test_queue", max_attempts=5)
        class TestWorker:
            def perform(self, _job):
                return None

        return TestWorker

    def test_creates_job_with_correct_attributes(self, basic_worker):
        job = basic_worker.new({"user_id": 123})

        assert isinstance(job, Job)
        assert job.args == {"user_id": 123}
        assert job.worker.endswith("TestWorker")
        assert job.queue == "test_queue"
        assert job.max_attempts == 5

    def test_applies_overrides_correctly(self, basic_worker):
        job = basic_worker.new({}, priority=1, queue="special")

        assert job.args == {}
        assert job.worker.endswith("TestWorker")
        assert job.priority == 1
        assert job.queue == "special"
