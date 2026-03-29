SELECT toDate(eventDate),
       count() AS rows
FROM l4_dataset
GROUP BY eventDate
ORDER BY eventDate;

SELECT formatReadableSize(sum(data_compressed_bytes))   AS compressed,
       formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
       sum(rows)                                        AS rows
FROM system.parts
WHERE table = 'l4_dataset';

SELECT column,
       formatReadableSize(sum(column_data_compressed_bytes)) AS size,
       sum(column_data_compressed_bytes)                     AS compressed
FROM system.parts_columns
WHERE table = 'l4_dataset'
GROUP BY column
ORDER BY compressed DESC;

SELECT geoCountry AS country,
       count()    AS events
FROM l4_dataset
WHERE geoCountry != ''
GROUP BY country;

SELECT platform,
       count() AS events
FROM l4_dataset
WHERE platform != ''
GROUP BY platform

SELECT toDate(eventDate)       AS date,
       uniqExact(userPseudoId) AS unique_users
FROM l4_dataset
GROUP BY date
ORDER BY date;