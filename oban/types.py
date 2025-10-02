from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar

T = TypeVar("T")


class JobState(StrEnum):
    """Represents the lifecycle state of a job.

    - AVAILABLE: ready to be executed
    - CANCELLED: explicitly cancelled
    - COMPLETED: successfully finished
    - DISCARDED: exceeded max attempts
    - EXECUTING: currently executing
    - RETRYABLE: failed but will be retried
    - SCHEDULED: scheduled to run in the future
    - SUSPENDED: not available to run currently
    """

    AVAILABLE = "available"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    DISCARDED = "discarded"
    EXECUTING = "executing"
    RETRYABLE = "retryable"
    SCHEDULED = "scheduled"
    SUSPENDED = "suspended"


@dataclass(frozen=True, slots=True)
class Snooze:
    seconds: int


@dataclass(frozen=True, slots=True)
class Cancel:
    reason: str


type Result[T] = Snooze | Cancel | None
