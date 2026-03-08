CREATE TABLE IF NOT EXISTS fracic.daily_event_activity
(
    event_date  String,
    event_name  String,
    geo_country Nullable(String),
    platform    Nullable(Enum8('ANDROID' = 1, 'IOS' = 2, 'WEB' = 3, '<all>' = 4)),
    count       UInt64,
    user_count  UInt64
)
    engine = MergeTree
        PARTITION BY event_date
        ORDER BY (event_name)
        SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS fracic.monthly_event_activity
(
    event_date  String,
    event_name  String,
    geo_country Nullable(String),
    platform    Nullable(Enum8('ANDROID' = 1, 'IOS' = 2, 'WEB' = 3, '<all>' = 4)),
    count       UInt64,
    user_count  UInt64
)
    engine = MergeTree
        PARTITION BY event_date
        ORDER BY (event_name)
        SETTINGS index_granularity = 8192;

INSERT INTO fracic.daily_event_activity
SELECT event_date,
       if(event_name = '', '<all>', event_name)                          AS event_name,
       if(geo_country IS NULL OR geo_country = '', '<all>', geo_country) AS geo_country,
       if(platform IS NULL, '<all>', toString(platform))                 AS platform,
       count,
       user_count
FROM (
         SELECT event_date,
                event_name,
                geo_country,
                platform,
                SUM(count)                     AS count,
                COUNT(DISTINCT user_pseudo_id) AS user_count
         FROM fracic.daily_user_activity
         GROUP BY event_date, event_name, geo_country, platform
         WITH CUBE
         HAVING event_date != ''
);

INSERT INTO fracic.monthly_event_activity
SELECT event_date,
       if(event_name = '', '<all>', event_name)                          AS event_name,
       if(geo_country IS NULL OR geo_country = '', '<all>', geo_country) AS geo_country,
       if(platform IS NULL, '<all>', toString(platform))                 AS platform,
       count,
       user_count
FROM (
         SELECT event_date,
                event_name,
                geo_country,
                platform,
                SUM(count)                     AS count,
                COUNT(DISTINCT user_pseudo_id) AS user_count
         FROM fracic.monthly_user_activity
         GROUP BY event_date, event_name, geo_country, platform
         WITH CUBE
         HAVING event_date != ''
);