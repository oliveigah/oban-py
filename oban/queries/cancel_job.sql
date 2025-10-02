UPDATE
  oban_jobs
SET
  state = 'cancelled'::oban_job_state,
  cancelled_at = timezone('UTC', now()),
  errors = errors || jsonb_build_object('attempt', %(attempt)s, 'at', timezone('UTC', now()), 'error', %(reason)s::text)
WHERE
  id = %(id)s
  AND state NOT IN ('completed', 'discarded', 'cancelled')
