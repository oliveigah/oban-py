UPDATE
  oban_jobs
SET
  args = %(args)s,
  max_attempts = %(max_attempts)s,
  meta = %(meta)s,
  priority = %(priority)s,
  queue = %(queue)s,
  scheduled_at = %(scheduled_at)s,
  state = CASE
    WHEN state = 'available' AND %(scheduled_at)s > timezone('UTC', now())
    THEN 'scheduled'::oban_job_state
    WHEN state = 'scheduled' AND %(scheduled_at)s <= timezone('UTC', now())
    THEN 'available'::oban_job_state
    ELSE state
  END,
  tags = %(tags)s,
  worker = %(worker)s
WHERE
  id = %(id)s
RETURNING
  args,
  max_attempts,
  meta,
  priority,
  queue,
  scheduled_at,
  state,
  tags,
  worker;
