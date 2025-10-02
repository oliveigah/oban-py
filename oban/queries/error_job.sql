UPDATE
  oban_jobs
SET
  state = CASE WHEN attempt >= max_attempts
          THEN 'discarded'::oban_job_state
          ELSE 'retryable'::oban_job_state
          END,
  discarded_at = CASE WHEN attempt >= max_attempts THEN timezone('UTC', now())
                 ELSE discarded_at
                 END,
  scheduled_at = CASE WHEN attempt >= max_attempts
                 THEN scheduled_at
                 ELSE timezone('UTC', now()) + format('%%s seconds', %(seconds)s)::interval
                 END,
  errors = errors || jsonb_build_object('attempt', %(attempt)s, 'at', timezone('UTC', now()), 'error', %(error)s::text)
WHERE
  id = %(id)s
