SELECT entity,
       name,
       weekly_count
FROM (SELECT entity,
             name,
             weekly_count,
             row_number() OVER (PARTITION BY entity ORDER BY weekly_count DESC) AS rn
      FROM (SELECT entity,
                   name,
                   sum(count) AS weekly_count
            FROM fracic.daily_entity_follows
            WHERE toDate(parseDateTime(event_date, '%Y%m%d'))
                      BETWEEN toDate('2024-01-15') AND toDate('2024-01-21')
            GROUP BY entity, name
               )
         )
WHERE rn <= 10
ORDER BY entity ASC, rn ASC;