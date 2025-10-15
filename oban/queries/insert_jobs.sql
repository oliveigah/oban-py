WITH raw_job_data AS (
    SELECT
        unnest(%(args)s::jsonb[]) AS args,
        unnest(%(inserted_at)s::timestamptz[]) AS inserted_at,
        unnest(%(max_attempts)s::smallint[]) AS max_attempts,
        unnest(%(meta)s::jsonb[]) AS meta,
        unnest(%(priority)s::smallint[]) AS priority,
        unnest(%(queue)s::text[]) AS queue,
        unnest(%(scheduled_at)s::timestamptz[]) AS scheduled_at,
        unnest(%(state)s::text[]) AS state,
        unnest(%(tags)s::jsonb[]) AS tags,
        unnest(%(worker)s::text[]) AS worker
)
INSERT INTO oban_jobs(
    args,
    inserted_at,
    max_attempts,
    meta,
    priority,
    queue,
    scheduled_at,
    state,
    tags,
    worker
) SELECT
    args,
    coalesce(inserted_at, timezone('UTC', now())) AS inserted_at,
    max_attempts,
    meta,
    priority,
    queue,
    coalesce(scheduled_at, timezone('UTC', now())) AS scheduled_at,
    CASE
        WHEN state = 'available' AND scheduled_at IS NOT NULL
        THEN 'scheduled'::oban_job_state
        ELSE state::oban_job_state
    END AS state,
    tags,
    worker
FROM raw_job_data
RETURNING id, inserted_at, queue, scheduled_at, state;
