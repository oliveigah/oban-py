DELETE FROM
  oban_leaders
WHERE
  expires_at < timezone('UTC', now())
