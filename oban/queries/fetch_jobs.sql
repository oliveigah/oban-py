WITH available_jobs AS (
  SELECT priority, scheduled_at, id
  FROM oban_jobs
  WHERE state = 'available'
    AND queue = %(queue)s
  ORDER BY priority ASC, scheduled_at ASC, id ASC
  LIMIT %(demand)s
  FOR UPDATE SKIP LOCKED
),
scheduled_jobs AS (
  SELECT priority, scheduled_at, id
  FROM oban_jobs
  WHERE state = 'scheduled'
    AND queue = %(queue)s
    AND scheduled_at <= timezone('UTC', now())
  ORDER BY priority ASC, scheduled_at ASC, id ASC
  LIMIT %(demand)s
  FOR UPDATE SKIP LOCKED
),
retryable_jobs AS (
  SELECT priority, scheduled_at, id
  FROM oban_jobs
  WHERE state = 'retryable'
    AND queue = %(queue)s
    AND scheduled_at <= timezone('UTC', now())
  ORDER BY priority ASC, scheduled_at ASC, id ASC
  LIMIT %(demand)s
  FOR UPDATE SKIP LOCKED
),
locked_jobs AS (
  SELECT * FROM available_jobs
  UNION ALL
  SELECT * FROM scheduled_jobs
  UNION ALL
  SELECT * FROM retryable_jobs
  ORDER BY priority ASC, scheduled_at ASC, id ASC
  LIMIT %(demand)s
)
UPDATE
  oban_jobs
SET
  attempt = oban_jobs.attempt + 1,
  attempted_at = timezone('UTC', now()),
  attempted_by = %(attempted_by)s,
  state = 'executing'
FROM
  locked_jobs
WHERE
  oban_jobs.id = locked_jobs.id
RETURNING
  oban_jobs.*
