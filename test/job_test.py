import pytest

from oban.job import Job


class TestJobValidation:
    def test_queue_validation(self):
        assert Job(worker="test.Worker", queue="default")

        with pytest.raises(ValueError, match="queue"):
            Job(worker="test.Worker", queue="")

        with pytest.raises(ValueError, match="queue"):
            Job(worker="test.Worker", queue="a" * 129)

    def test_worker_validation(self):
        assert Job(worker="test.Worker")

        with pytest.raises(ValueError, match="worker"):
            Job(worker="")

        with pytest.raises(ValueError, match="worker"):
            Job(worker="a" * 129)

    def test_max_attempts_validation(self):
        assert Job(worker="test.Worker", max_attempts=1)
        assert Job(worker="test.Worker", max_attempts=20)

        with pytest.raises(ValueError, match="max_attempts"):
            Job(worker="test.Worker", max_attempts=0)

        with pytest.raises(ValueError, match="max_attempts"):
            Job(worker="test.Worker", max_attempts=-1)

    def test_priority_validation(self):
        assert Job(worker="test.Worker", priority=0)

        with pytest.raises(ValueError, match="priority"):
            Job(worker="test.Worker", priority=-1)

        with pytest.raises(ValueError, match="priority"):
            Job(worker="test.Worker", priority=10)
