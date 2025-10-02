UPDATE
  oban_jobs
SET
  state = 'completed',
  completed_at = timezone('UTC', now())
WHERE
  id = %(id)s;

