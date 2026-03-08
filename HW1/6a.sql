CREATE TABLE IF NOT EXISTS fracic.daily_user_activity
(
    event_date     String,
    event_name     String,
    user_pseudo_id String,
    geo_country    Nullable(String),
    platform       Nullable(Enum8('ANDROID' = 1, 'IOS' = 2, 'WEB' = 3)),
    count          UInt64
)
    engine = MergeTree
        PARTITION BY event_date
        ORDER BY (event_name)
        SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS fracic.monthly_user_activity
(
    event_date     String,
    event_name     String,
    user_pseudo_id String,
    geo_country    Nullable(String),
    platform       Nullable(Enum8('ANDROID' = 1, 'IOS' = 2, 'WEB' = 3)),
    count          UInt64
)
    engine = MergeTree
        PARTITION BY event_date
        ORDER BY (event_name)
        SETTINGS index_granularity = 8192;


INSERT INTO fracic.daily_user_activity
SELECT event_date, event_name, user_pseudo_id, geo_country, platform, COUNT(*)
FROM bq.events
WHERE event_date >= '20240201'
  AND event_date < '20240301'
GROUP BY event_date, event_name, user_pseudo_id, geo_country, platform;

INSERT INTO fracic.monthly_user_activity
SELECT formatDateTime(toStartOfMonth(toDate(event_date)), '%Y%m%d'),
       event_name,
       user_pseudo_id,
       geo_country,
       platform,
       COUNT(*)
FROM bq.events
WHERE event_date >= '20240201'
  AND event_date < '20240301'
GROUP BY formatDateTime(toStartOfMonth(toDate(event_date)), '%Y%m%d'), event_name, user_pseudo_id, geo_country, platform;
