-- Types

CREATE TYPE oban_job_state AS ENUM (
    'available',
    'scheduled',
    'suspended',
    'executing',
    'retryable',
    'completed',
    'discarded',
    'cancelled'
);

-- Tables

CREATE TABLE oban_jobs (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    state oban_job_state NOT NULL DEFAULT 'available',
    queue text NOT NULL DEFAULT 'default',
    worker text NOT NULL,
    attempt smallint NOT NULL DEFAULT 0,
    max_attempts smallint NOT NULL DEFAULT 20,
    priority smallint NOT NULL DEFAULT 0,
    args jsonb NOT NULL DEFAULT '{}',
    meta jsonb NOT NULL DEFAULT '{}',
    tags jsonb NOT NULL DEFAULT '[]',
    errors jsonb NOT NULL DEFAULT '[]',
    attempted_by text[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    inserted_at timestamp WITHOUT TIME ZONE NOT NULL DEFAULT timezone('UTC', now()),
    scheduled_at timestamp WITHOUT TIME ZONE NOT NULL DEFAULT timezone('UTC', now()),
    attempted_at timestamp WITHOUT TIME ZONE,
    cancelled_at timestamp WITHOUT TIME ZONE,
    completed_at timestamp WITHOUT TIME ZONE,
    discarded_at timestamp WITHOUT TIME ZONE,
    CONSTRAINT attempt_range CHECK (attempt >= 0 AND attempt <= max_attempts),
    CONSTRAINT queue_length CHECK (char_length(queue) > 0),
    CONSTRAINT worker_length CHECK (char_length(worker) > 0),
    CONSTRAINT positive_max_attempts CHECK (max_attempts > 0),
    CONSTRAINT non_negative_priority CHECK (priority >= 0)
);

CREATE UNLOGGED TABLE oban_leaders (
    name text PRIMARY KEY DEFAULT 'oban',
    node text NOT NULL,
    elected_at timestamp WITHOUT TIME ZONE NOT NULL,
    expires_at timestamp WITHOUT TIME ZONE NOT NULL
);

-- Indexes

CREATE INDEX oban_jobs_state_queue_priority_scheduled_at_id_index
ON oban_jobs (state, queue, priority, scheduled_at, id);

CREATE INDEX oban_jobs_staging_index
ON oban_jobs (scheduled_at, id)
WHERE state IN ('scheduled', 'retryable');

CREATE INDEX oban_jobs_meta_index
ON oban_jobs USING gin (meta);

CREATE INDEX oban_jobs_completed_at_index
ON oban_jobs (completed_at)
WHERE state = 'completed';

CREATE INDEX oban_jobs_cancelled_at_index
ON oban_jobs (cancelled_at)
WHERE state = 'cancelled';

CREATE INDEX oban_jobs_discarded_at_index
ON oban_jobs (discarded_at)
WHERE state = 'discarded';

-- Autovacuum

ALTER TABLE oban_jobs SET (
  -- Vacuum early, at 5% dead or 1000 dead rows
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_vacuum_threshold = 1000,

  -- Analyze even earlier to keep the planner up to date
  autovacuum_analyze_scale_factor = 0.025,
  autovacuum_analyze_threshold = 500,

  -- Run vacuum more aggressively with minimal sleep and 10x the default IO
  autovacuum_vacuum_cost_limit = 2000,
  autovacuum_vacuum_cost_delay = 10,

  -- Reserve 10% free space per page for HOT updates
  fillfactor = 90
);
