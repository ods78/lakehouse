CREATE DATABASE IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.events
(
    id Int64, user_id Int32, event_type String, amount Float64, ts DateTime
)
ENGINE = MergeTree
ORDER BY (user_id, ts)
TTL ts + INTERVAL 30 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 'hot_cold';
