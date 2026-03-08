SELECT e.id, COUNT(*) AS number_of_openings
FROM bq.events e
         JOIN sports.event se ON e.id = se.id
         JOIN sports.tournament t ON se.tournament_id = t.id
         JOIN sports.uniquetournament ut ON t.uniquetournament_id = ut.id
WHERE e.event_name = 'open_event'
  AND ut.name = 'HNL'
  AND YEAR(ut.startdate) = 2024
  AND YEAR(ut.enddate) = 2025
GROUP BY e.id
ORDER BY COUNT(*) DESC
LIMIT 1