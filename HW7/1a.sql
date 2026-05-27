SELECT ev_date,
       avg(events_per_user) AS avg_events_per_user
FROM (
         SELECT e.event_date     AS ev_date,
                e.user_pseudo_id AS user_id,
                uniqExact(e.id)  AS events_per_user
         FROM bq.events AS e
                  INNER JOIN sports.event AS se ON e.id = se.id
         WHERE e.event_name = 'open_event'
           AND e.platform = 'IOS'
           AND e.geo_country = 'Croatia'
           AND se.sport_id = 1
           AND e.event_date BETWEEN '20240101' AND '20240530'
         GROUP BY ev_date, user_id
         )
GROUP BY ev_date
ORDER BY ev_date