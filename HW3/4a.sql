SELECT week,
       sport,
       weekly_count,
       rank() OVER (PARTITION BY week ORDER BY weekly_count DESC) AS rank
FROM (SELECT toMonday(toDate(parseDateTime(event_date, '%Y%m%d'))) AS week,
             dictGet('fracic.sport_dictionary', 'name',
                     dictGet('fracic.event_dictionary', 'sport_id', event_id)
             )                                                     AS sport,
             sum(count)                                            AS weekly_count
      FROM fracic.daily_event_openings
      WHERE toMonday(toDate(parseDateTime(event_date, '%Y%m%d')))
                BETWEEN toDate('2024-01-01') AND toDate('2024-01-22')
      GROUP BY week, sport)
ORDER BY week ASC, rank ASC;