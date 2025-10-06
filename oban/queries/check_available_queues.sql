SELECT DISTINCT queue
FROM oban_jobs
WHERE state = 'available'
UNION
SELECT DISTINCT queue
FROM oban_jobs
WHERE state = 'scheduled'
AND scheduled_at <= timezone('utc', now())
UNION
SELECT DISTINCT queue
FROM oban_jobs
WHERE state = 'retryable'
AND scheduled_at <= timezone('utc', now())
