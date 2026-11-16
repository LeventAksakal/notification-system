-- Per-minute rollups of delivery outcomes, read by GET /v1/metrics (decision
-- 0014, "As built"). Only the triggers in notifications.sql write them, in the
-- transaction that records the outcome, so they never disagree with the rows
-- they count. The read sums whole minutes here and reads the window's leading
-- partial minute from the raw rows, on the same timestamps.
--
-- shard spreads one key over 4 rows while its minute is written: an outcome
-- counts into shard (attempt id mod 4) or (CRC32 of the notification id mod
-- 4), and a change takes its old contribution out of the key's first shard
-- that holds any (only the sum over shards means anything). Why: a rollup row
-- is locked from the write that counts an outcome until that transaction
-- commits (its fsync included), so one row takes one finish at a time.
-- Unsharded, every finish of a channel in a minute met on its accepted row:
-- 128 to 130 finishes/s on one channel against 344 to 420/s without the
-- triggers, 1,300 row lock waits (64 s waited) per 10 s, and the rate test's
-- steady state fell from 95 to 58-84 sends/s per channel. Sharding only
-- attempts_per_minute left a channel's failures, or a backlog whose latencies
-- share a bucket, on one finishes row: 80 to 88 finishes/s. Both sharded:
-- 256 to 276/s, and the rate test holds 95/s (measured 16 Nov 2026,
-- TestMeasureRollupWriteCost, TestWorkersHoldTheRateAtTheProvider). More
-- shards cut the waits further but multiply the rows of the minutes being
-- written. The retention pass folds a minute's shards into shard 0 two
-- minutes after it starts (store.FoldRollups): sharded rows had multiplied
-- the rollup rows of an hour 2.75 times and its read from 14 to 34 ms.

-- Provider calls by the minute they started (delivery_attempts.started_at, the
-- worker's clock, truncated to the minute), channel and outcome. An attempt is
-- counted once it has an outcome; a later outcome moves its count.
CREATE TABLE attempts_per_minute (
  minute DATETIME NOT NULL,
  channel ENUM('sms', 'email', 'push') NOT NULL,
  outcome ENUM('accepted', 'transient', 'permanent', 'unknown') NOT NULL,
  shard TINYINT UNSIGNED NOT NULL,
  n INT NOT NULL,
  PRIMARY KEY (minute, channel, outcome, shard),
  CONSTRAINT attempts_per_minute_whole_minute CHECK ((second(`minute`) = 0)),
  CONSTRAINT attempts_per_minute_shard CHECK ((`shard` < 4)),
  CONSTRAINT attempts_per_minute_count CHECK ((`n` >= 0))
);

-- Notifications that reached sent or failed, by the minute they finished
-- (notifications.finished_at, the database clock), channel and status. A sent
-- row also carries the bucket of its accept-to-sent latency (from the first due
-- time, as the endpoint has always measured it): latency_le_ms is the bucket's
-- inclusive upper bound in ms (internal/store/latency_buckets.go), 0 for failed;
-- BIGINT because a backlog older than 49 days would overflow INT and fail the
-- write that finishes the notification.
CREATE TABLE finishes_per_minute (
  minute DATETIME NOT NULL,
  channel ENUM('sms', 'email', 'push') NOT NULL,
  status ENUM('sent', 'failed') NOT NULL,
  latency_le_ms BIGINT UNSIGNED NOT NULL,
  shard TINYINT UNSIGNED NOT NULL,
  n INT NOT NULL,
  PRIMARY KEY (minute, channel, status, latency_le_ms, shard),
  CONSTRAINT finishes_per_minute_whole_minute CHECK ((second(`minute`) = 0)),
  CONSTRAINT finishes_per_minute_shard CHECK ((`shard` < 4)),
  CONSTRAINT finishes_per_minute_count CHECK ((`n` >= 0)),
  CONSTRAINT finishes_per_minute_latency_only_when_sent CHECK (((`status` = _utf8mb4'sent') or (`latency_le_ms` = 0)))
);
