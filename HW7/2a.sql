CREATE TABLE IF NOT EXISTS fracic.hw7_forecast
(
    ev_date    Date,
    method     LowCardinality(String),
    actual     Nullable(Float64),
    forecast   Nullable(Float64),
    wow_change Nullable(Float64)
)
    ENGINE = MergeTree
        ORDER BY (method, ev_date);