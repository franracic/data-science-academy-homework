SELECT s.name AS sport, COUNT(*)
FROM bq.events e
         JOIN sports.event se ON e.id = se.id
         JOIN sports.sport s ON se.sport_id = s.id
WHERE event_name = 'open_event'
  AND e.event_date >= '20240101'
  and e.event_date < '20240201'
GROUP BY s.name
ORDER BY s.name