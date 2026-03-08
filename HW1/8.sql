SELECT e.geo_country,
       argMax(p.name, c) AS player
FROM (
         SELECT geo_country,
                id,
                COUNT(*) AS c
         FROM bq.events
         WHERE event_name = 'follow_player'
           AND event_date >= '20240201'
           AND event_date < '20240301'
         GROUP BY geo_country, id
         ) e
INNER JOIN sports.player p ON e.id = p.id
GROUP BY e.geo_country
ORDER BY e.geo_country;