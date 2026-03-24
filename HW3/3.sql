CREATE TABLE IF NOT EXISTS fracic.daily_event_openings
(
    event_date String,
    event_id   UInt64,
    count      SimpleAggregateFunction(sum, UInt64)
)
    ENGINE = AggregatingMergeTree()
        ORDER BY (event_date, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS fracic.mv_daily_event_openings
    TO fracic.daily_event_openings AS
SELECT event_date,
       toUInt64(id) AS event_id,
       count()      AS count
FROM bq.events
WHERE event_name = 'open_event'
GROUP BY event_date, event_id;

INSERT INTO fracic.daily_event_openings
SELECT event_date,
       toUInt64(id) AS event_id,
       count()      AS count
FROM bq.events
WHERE event_name = 'open_event'
GROUP BY event_date, event_id;