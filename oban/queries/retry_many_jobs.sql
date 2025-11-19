UPDATE
  oban_jobs
SET
  state = 'available'::oban_job_state,
  max_attempts = GREATEST(max_attempts, attempt + 1),
  scheduled_at = timezone('UTC', now()),
  completed_at = NULL,
  cancelled_at = NULL,
  discarded_at = NULL,
  meta = jsonb_set(meta, '{uniq_bmp}', '[]'::jsonb)
WHERE
  id = ANY(%(ids)s)
  AND state NOT IN ('available', 'executing')
