import enum

from sqlalchemy import Column, CheckConstraint, Enum, Index, Table
from sqlalchemy import ARRAY, BIGINT, SMALLINT, TEXT, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from .schema import metadata


class State(enum.Enum):
    available = "available"  # Job is ready to be executed
    scheduled = "scheduled"  # Job is scheduled to run in the future
    executing = "executing"  # Job is currently being executed
    retryable = "retryable"  # Job has failed but will be retried
    completed = "completed"  # Job has successfully finished execution
    discarded = "discarded"  # Job has been discarded after exceeding max attempts
    cancelled = "cancelled"  # Job has been explicitly cancelled


table = Table(
    "oban_jobs",
    metadata,
    Column("id", BIGINT, primary_key=True),
    Column(
        "state",
        Enum(State, name="oban_job_state"),
        nullable=False,
        server_default="available",
    ),
    Column("queue", TEXT, nullable=False, server_default="default"),
    Column("worker", TEXT, nullable=False),
    Column("attempt", SMALLINT, nullable=False, server_default="0"),
    Column("max_attempts", SMALLINT, nullable=False, server_default="20"),
    Column("priority", SMALLINT, nullable=False, server_default="0"),
    Column("args", JSONB, nullable=False, server_default="'{}'"),
    Column("meta", JSONB, nullable=False, server_default="'{}'"),
    Column("tags", JSONB, nullable=False, server_default="'[]'"),
    Column("errors", JSONB, nullable=False, server_default="'[]'"),
    Column("attempted_by", ARRAY(TEXT), nullable=False),
    Column(
        "inserted_at",
        TIMESTAMP,
        nullable=False,
        server_default="timezone('UTC', now())",
    ),
    Column(
        "scheduled_at",
        TIMESTAMP,
        nullable=False,
        server_default="timezone('UTC', now())",
    ),
    Column("attempted_at", TIMESTAMP),
    Column("cancelled_at", TIMESTAMP),
    Column("completed_at", TIMESTAMP),
    Column("discarded_at", TIMESTAMP),
    # Check Constraints
    CheckConstraint("attempt >= 0 AND attempt <= max_attempts", name="attempt_range"),
    CheckConstraint("priority >= 0", name="non_negative_priority"),
    CheckConstraint("max_attempts > 0", name="positive_max_attempts"),
    CheckConstraint("char_length(queue) > 0", name="queue_length"),
    CheckConstraint("char_length(worker) > 0", name="worker_length"),
    # Index Constraints
    Index("oban_jobs_args_index", "args", postgresql_using="gin"),
    Index("oban_jobs_meta_index", "meta", postgresql_using="gin"),
    Index(
        "oban_jobs_state_queue_priority_scheduled_at_id_index",
        "state",
        "queue",
        "priority",
        "scheduled_at",
        "id",
    ),
)
