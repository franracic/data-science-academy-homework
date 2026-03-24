/* CREATE TABLE IF NOT EXISTS fracic.daily_event_activity
(
    event_date  String CODEC (ZSTD(1)),
    event_name  String CODEC (ZSTD(1)),
    geo_country String CODEC (ZSTD(1)),
    platform    Enum8('ANDROID' = 1, 'IOS' = 2, 'WEB' = 3, '<all>' = 4) CODEC (ZSTD(1)),
    count       UInt64 CODEC (T64, ZSTD(1)),
    user_count  UInt64 CODEC (T64, ZSTD(1))
)
    ENGINE = MergeTree()
        PARTITION BY event_date
        ORDER BY (event_name, geo_country, platform)
        SETTINGS index_granularity = 8192;

INSERT INTO fracic.daily_event_activity
SELECT event_date,
       if(event_name = '', '<all>', event_name)                          AS event_name,
       if(geo_country IS NULL OR geo_country = '', '<all>', geo_country) AS geo_country,
       if(platform IS NULL, '<all>', toString(platform))                 AS platform,
       sum(count)                                                        AS count,
       uniqExact(user_pseudo_id)                                         AS user_count
FROM aggregations.daily_user_activity
GROUP BY event_date, event_name, geo_country, platform
WITH CUBE
HAVING event_date != '';

CREATE TABLE IF NOT EXISTS fracic.monthly_event_activity
(
    event_date  String CODEC (ZSTD(1)),
    event_name  String CODEC (ZSTD(1)),
    geo_country String CODEC (ZSTD(1)),
    platform    Enum8('ANDROID' = 1, 'IOS' = 2, 'WEB' = 3, '<all>' = 4) CODEC (ZSTD(1)),
    count       UInt64 CODEC (T64, ZSTD(1)),
    user_count  UInt64 CODEC (T64, ZSTD(1))
)
    ENGINE = MergeTree()
        PARTITION BY event_date
        ORDER BY (event_name, geo_country, platform)
        SETTINGS index_granularity = 8192;

INSERT INTO fracic.monthly_event_activity
SELECT event_date,
       if(event_name = '', '<all>', event_name)                          AS event_name,
       if(geo_country IS NULL OR geo_country = '', '<all>', geo_country) AS geo_country,
       if(platform IS NULL, '<all>', toString(platform))                 AS platform,
       sum(count)                                                        AS count,
       uniqExact(user_pseudo_id)                                         AS user_count
FROM aggregations.monthly_user_activity
GROUP BY event_date, event_name, geo_country, platform
WITH CUBE
HAVING event_date != ''; */

CREATE TABLE IF NOT EXISTS fracic.daily_event_activity
(
    event_date  String CODEC (ZSTD(1)),
    event_name  String CODEC (ZSTD(1)),
    geo_country String CODEC (ZSTD(1)),
    platform    Enum8('ANDROID' = 1, 'IOS' = 2, 'WEB' = 3, '<all>' = 4) CODEC (ZSTD(1)),
    count       UInt64 CODEC (T64),
    user_count  UInt64 CODEC (T64)
)
    ENGINE = MergeTree()
        PARTITION BY substring(event_date, 1, 6)
        ORDER BY (event_name, geo_country, platform)
        SETTINGS index_granularity = 8192;

INSERT INTO fracic.daily_event_activity
SELECT event_date,
       if(event_name = '', '<all>', event_name)   AS event_name,
       if(geo_country = '', '<all>', geo_country) AS geo_country,
       if(platform = '', '<all>', platform)       AS platform,
       count,
       user_count
FROM (
         SELECT event_date,
                event_name,
                if(geo_country IS NULL, '', toString(geo_country)) AS geo_country,
                if(platform IS NULL, '', toString(platform))       AS platform,
                sum(count)                                         AS count,
                uniqExact(user_pseudo_id)                          AS user_count
         FROM aggregations.daily_user_activity
         WHERE event_date != ''
         GROUP BY GROUPING SETS (
             (event_date, event_name, geo_country, platform),
             ( event_date, event_name, geo_country),
             ( event_date, event_name, platform),
             ( event_date, geo_country, platform),
             ( event_date, event_name),
             ( event_date, geo_country),
             ( event_date, platform),
             ( event_date)
             )
         );

CREATE TABLE IF NOT EXISTS fracic.monthly_event_activity
(
    event_date  String CODEC (ZSTD(1)),
    event_name  String CODEC (ZSTD(1)),
    geo_country String CODEC (ZSTD(1)),
    platform    Enum8('ANDROID' = 1, 'IOS' = 2, 'WEB' = 3, '<all>' = 4) CODEC (ZSTD(1)),
    count       UInt64 CODEC (T64),
    user_count  UInt64 CODEC (T64)
)
    ENGINE = MergeTree()
        PARTITION BY substring(event_date, 1, 6)
        ORDER BY (event_name, geo_country, platform)
        SETTINGS index_granularity = 8192;

INSERT INTO fracic.monthly_event_activity
SELECT event_date,
       if(event_name = '', '<all>', event_name)   AS event_name,
       if(geo_country = '', '<all>', geo_country) AS geo_country,
       if(platform = '', '<all>', platform)       AS platform,
       count,
       user_count
FROM (
         SELECT event_date,
                event_name,
                if(geo_country IS NULL, '', toString(geo_country)) AS geo_country,
                if(platform IS NULL, '', toString(platform))       AS platform,
                sum(count)                                         AS count,
                uniqExact(user_pseudo_id)                          AS user_count
         FROM aggregations.monthly_user_activity
         WHERE event_date != ''
         GROUP BY GROUPING SETS (
             (event_date, event_name, geo_country, platform),
             ( event_date, event_name, geo_country),
             ( event_date, event_name, platform),
             ( event_date, geo_country, platform),
             ( event_date, event_name),
             ( event_date, geo_country),
             ( event_date, platform),
             ( event_date)
             )
         );