WITH jobs_to_delete AS (
  SELECT
    id
  FROM
    oban_jobs
  WHERE
    (state = 'completed' AND completed_at <= timezone('UTC', now()) - make_interval(secs => %(max_age)s)) OR
    (state = 'cancelled' AND cancelled_at <= timezone('UTC', now()) - make_interval(secs => %(max_age)s)) OR
    (state = 'discarded' AND discarded_at <= timezone('UTC', now()) - make_interval(secs => %(max_age)s))
  ORDER BY
    id ASC
  LIMIT
    %(limit)s
)
DELETE FROM
  oban_jobs
WHERE
  id IN (SELECT id FROM jobs_to_delete)
