SELECT
  state::text,
  queue,
  COUNT(*)::integer
FROM
  oban_jobs
WHERE
  state = ANY(%(states)s::oban_job_state[])
GROUP BY
  state, queue
