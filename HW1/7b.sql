SELECT event_date
FROM fracic.daily_event_activity
WHERE event_name = 'open_event'
  AND geo_country = 'Croatia'
  AND platform = '<all>'
ORDER BY count DESC
LIMIT 1