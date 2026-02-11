WITH queues AS (
  SELECT DISTINCT queue FROM oban_producers
)
SELECT
  x.state,
  q.queue,
  oban_count_estimate(x.state, q.queue)::integer
FROM
  json_array_elements_text(%(states)s::json) AS x(state)
CROSS JOIN
  queues q
