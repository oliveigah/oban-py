UPDATE
  oban_jobs
SET
  attempt = attempt - 1,
  state = 'scheduled'::oban_job_state,
  scheduled_at = timezone('UTC', now()) + make_interval(secs => %(seconds)s),
  meta = meta || jsonb_build_object('snoozed', coalesce((meta->>'snoozed')::int, 0) + 1)
WHERE
  id = %(id)s
