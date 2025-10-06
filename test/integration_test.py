import pytest
import time
from datetime import datetime, timedelta, timezone

from oban import Oban, Cancel, Snooze, worker, _query


@worker()
class Worker:
    performed = set()

    def perform(self, job):
        Worker.performed.add(job.args["ref"])

        match job.args:
            case {"act": "er"}:
                raise RuntimeError("this failed")
            case {"act": "ca"}:
                return Cancel("no reason")
            case {"act": "sn"}:
                return Snooze(1)
            case _:
                return None


def with_backoff(check_fn, timeout=1.0, interval=0.01):
    """Retry a check function with exponential backoff until it passes or times out."""
    start = time.time()
    last_error = None

    while time.time() - start < timeout:
        try:
            check_fn()
            return
        except AssertionError as error:
            last_error = error
            time.sleep(interval)

    if last_error:
        raise last_error


class TestObanIntegration:
    @pytest.fixture(autouse=True)
    def setup(self, db_url):
        self.db_url = db_url

    def teardown_method(self):
        Worker.performed.clear()

    def oban_instance(self, **overrides):
        params = {
            "pool": {"url": self.db_url},
            "queues": {"default": 2},
            "stage_interval": 0.1,
        }

        return Oban(**{**params, **overrides})

    def assert_performed(self, ref):
        assert ref in Worker.performed

    def get_job(self, oban, job_id):
        with oban.get_connection() as conn:
            return _query.get_job(conn, job_id)

    def assert_job_state(self, oban, job_id, expected_state):
        job = self.get_job(oban, job_id)

        assert job is not None and job.state == expected_state

    def test_inserting_and_executing_jobs(self):
        with self.oban_instance() as oban:
            job_1 = Worker.enqueue({"act": "ok", "ref": 1})
            job_2 = Worker.enqueue({"act": "er", "ref": 2})
            job_3 = Worker.enqueue({"act": "ca", "ref": 3})
            job_4 = Worker.enqueue({"act": "sn", "ref": 4})
            job_5 = Worker.enqueue({"act": "er", "ref": 5}, max_attempts=1)

            with_backoff(lambda: self.assert_performed(1))
            with_backoff(lambda: self.assert_performed(2))
            with_backoff(lambda: self.assert_performed(3))
            with_backoff(lambda: self.assert_performed(4))
            with_backoff(lambda: self.assert_performed(5))

            with_backoff(lambda: self.assert_job_state(oban, job_1.id, "completed"))
            with_backoff(lambda: self.assert_job_state(oban, job_2.id, "retryable"))
            with_backoff(lambda: self.assert_job_state(oban, job_3.id, "cancelled"))
            with_backoff(lambda: self.assert_job_state(oban, job_4.id, "scheduled"))
            with_backoff(lambda: self.assert_job_state(oban, job_5.id, "discarded"))

    def test_executing_scheduled_jobs(self):
        with self.oban_instance() as oban:
            utc_now = datetime.now(timezone.utc)

            past_time = utc_now - timedelta(seconds=30)
            next_time = utc_now + timedelta(seconds=30)

            job_1 = Worker.enqueue({"ref": 1}, scheduled_at=past_time)
            job_2 = Worker.enqueue({"ref": 2}, scheduled_at=next_time)

            with_backoff(lambda: self.assert_performed(1))
            with_backoff(lambda: self.assert_job_state(oban, job_1.id, "completed"))

            self.assert_job_state(oban, job_2.id, "scheduled")

    def test_errored_jobs_are_retryable_with_backoff(self):
        with self.oban_instance() as oban:
            job = Worker.enqueue({"act": "er", "ref": 1})
            now = datetime.now(timezone.utc)

            with_backoff(lambda: self.assert_job_state(oban, job.id, "retryable"))

            job = self.get_job(oban, job.id)

            assert job.scheduled_at > now

            assert len(job.errors) > 0
            assert job.errors[0]["at"] is not None
            assert job.errors[0]["attempt"] == 1
            assert job.errors[0]["error"] is not None
