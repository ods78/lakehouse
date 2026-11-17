-- Hot-tier demo table: rows older than 7 days move to the 'cold' volume (Ceph S3 disk).
CREATE DATABASE IF NOT EXISTS lakehouse;

CREATE TABLE IF NOT EXISTS lakehouse.hot_events
(
    event_time  DateTime,
    customer_id Int64,
    event_type  LowCardinality(String),
    payload     String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, customer_id)
TTL event_time + INTERVAL 7 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered';

INSERT INTO lakehouse.hot_events
SELECT
    now() - toIntervalDay(number % 14),
    1000 + number,
    ['page_view','click','purchase','signup'][1 + number % 4],
    concat('{"customer_id":', toString(1000 + number), ',"src":"hot_tier_demo"}')
FROM numbers(1000);
