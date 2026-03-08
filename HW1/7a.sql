SELECT event_date
FROM fracic.daily_event_activity
WHERE event_name = '<all>'
  AND geo_country = '<all>'
  AND platform = '<all>'
ORDER BY user_count DESC
LIMIT 1