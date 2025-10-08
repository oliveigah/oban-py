WITH locked_jobs AS (
  SELECT
    id
  FROM
    oban_jobs
  WHERE
    state = ANY('{scheduled,retryable}')
    AND scheduled_at <= timezone('UTC', now())
  ORDER BY
    scheduled_at ASC, id ASC
  LIMIT
    %(limit)s
  FOR UPDATE SKIP LOCKED
)
UPDATE
  oban_jobs
SET
  state = 'available'::oban_job_state
FROM
  locked_jobs
WHERE
  oban_jobs.id = locked_jobs.id;
