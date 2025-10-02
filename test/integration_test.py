import pytest
import time

from oban import Oban, Cancel, Snooze


class TestObanIntegration:
    @pytest.fixture(autouse=True)
    def setup(self, db_url):
        self.performed = set()
        self.db_url = db_url
        self.oban = None

    def teardown_method(self):
        if self.oban:
            self.oban.stop()

    def with_backoff(self, check_fn, timeout=1.0, interval=0.01):
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

    def assert_performed(self, ref):
        assert ref in self.performed

    def assert_job_state(self, job_id: int, expected_state: str):
        with self.oban.get_connection() as conn:
            result = conn.execute(
                "SELECT state FROM oban_jobs WHERE id = %s", (job_id,)
            ).fetchone()
            actual_state = result[0] if result else None

        assert actual_state == expected_state

    def test_inserting_and_executing_jobs(self):
        performed = self.performed
        self.oban = Oban(pool={"url": self.db_url}, queues={"default": 2}).start()

        @self.oban.worker()
        class Worker:
            def perform(self, job):
                performed.add(job.args["ref"])

                match job.args:
                    case {"act": "er"}:
                        raise RuntimeError("this failed")
                    case {"act": "ca"}:
                        return Cancel("no reason")
                    case {"act": "sn"}:
                        return Snooze(1)
                    case _:
                        return None

        job_1 = Worker.enqueue({"act": "ok", "ref": 1})
        job_2 = Worker.enqueue({"act": "er", "ref": 2})
        job_3 = Worker.enqueue({"act": "ca", "ref": 3})
        job_4 = Worker.enqueue({"act": "sn", "ref": 4})
        job_5 = Worker.enqueue({"act": "er", "ref": 5}, max_attempts=1)

        self.with_backoff(lambda: self.assert_performed(1))
        self.with_backoff(lambda: self.assert_performed(2))
        self.with_backoff(lambda: self.assert_performed(3))
        self.with_backoff(lambda: self.assert_performed(4))
        self.with_backoff(lambda: self.assert_performed(5))

        self.with_backoff(lambda: self.assert_job_state(job_1.id, "completed"))
        self.with_backoff(lambda: self.assert_job_state(job_2.id, "retryable"))
        self.with_backoff(lambda: self.assert_job_state(job_3.id, "cancelled"))
        self.with_backoff(lambda: self.assert_job_state(job_4.id, "scheduled"))
        self.with_backoff(lambda: self.assert_job_state(job_5.id, "discarded"))
