SELECT
  *
FROM
  oban_jobs
WHERE
  state = ANY(%(states)s)
ORDER BY
  id DESC
