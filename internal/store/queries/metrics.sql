-- Every metrics statement runs in one read-only REPEATABLE READ transaction
-- and takes the database clock, read once with SELECT NOW(3), as an
-- argument, so the numbers of one summary agree with each other.

-- name: MetricsNow :one
SELECT CAST(NOW(3) AS DATETIME(3)) AS now;

-- name: MetricsQueue :many
-- Unfinished notifications per channel, read from claim_idx (status, channel,
-- priority, due_at) alone: an index range over the pending and processing
-- rows, no table reads. pending_due is what a worker may claim at now;
-- oldest_due_age_ms is how long the oldest of those has waited.
SELECT
  channel,
  CAST(COALESCE(SUM(status = 'pending' AND due_at <= sqlc.arg(now)), 0) AS SIGNED) AS pending_due,
  CAST(COALESCE(SUM(status = 'pending' AND due_at > sqlc.arg(now)), 0) AS SIGNED) AS pending_later,
  CAST(COALESCE(SUM(status = 'processing'), 0) AS SIGNED) AS processing,
  CAST(COALESCE(TIMESTAMPDIFF(MICROSECOND,
    MIN(CASE WHEN status = 'pending' AND due_at <= sqlc.arg(now) THEN due_at END), sqlc.arg(now)), 0) DIV 1000 AS SIGNED) AS oldest_due_age_ms
FROM notifications FORCE INDEX (claim_idx)
WHERE status IN ('pending', 'processing')
GROUP BY channel;

-- name: MetricsStatusCounts :many
-- Stored status counts over the whole table, from list_status_idx alone (an
-- index scan, no table reads).
SELECT status, COUNT(*) AS n
FROM notifications FORCE INDEX (list_status_idx)
GROUP BY status;

-- name: MetricsScheduled :one
-- Pending notifications shown as scheduled (never tried, due after now): a
-- claim_idx range over the not-yet-due pending rows, from the index alone
-- (attempts is its last column).
SELECT COUNT(*) AS n
FROM notifications FORCE INDEX (claim_idx)
WHERE status = 'pending' AND due_at > sqlc.arg(now) AND attempts = 0;

-- Delivery over a window [since, now] is read on the same timestamps the
-- rollup triggers truncate: whole minutes from the rollups, and the window's
-- partial first minute m0 from the raw rows, whichever side of since is
-- shorter: [since, m0 + 1 min) added to the rollups from m0 + 1 min, or
-- [m0, since) subtracted from the rollups from m0 (exact, as a rollup row
-- equals a recount of its rows). So at most 30 s of raw rows are read,
-- whatever the window; the current minute's rollup rows commit with the
-- outcomes they count.

-- name: MetricsAttemptRollups :many
-- Provider calls per channel and outcome in the whole minutes from
-- from_minute on (attempts_per_minute primary key range).
SELECT channel, outcome, CAST(SUM(n) AS SIGNED) AS n
FROM attempts_per_minute
WHERE minute >= sqlc.arg(from_minute)
GROUP BY channel, outcome;

-- name: MetricsFinishRollups :many
-- Notifications that reached sent or failed in the whole minutes from
-- from_minute on, per channel, status and latency bucket (finishes_per_minute
-- primary key range).
SELECT channel, status, latency_le_ms, CAST(SUM(n) AS SIGNED) AS n
FROM finishes_per_minute
WHERE minute >= sqlc.arg(from_minute)
GROUP BY channel, status, latency_le_ms;

-- name: MetricsAttemptsBetween :many
-- Provider calls started in [from_time, until_time), one row each, from
-- attempts_window_idx (started_at, channel, outcome) alone. The caller counts
-- them: at most half a minute of rows (metrics.go, halfMinute), and sending
-- them costs less than MySQL's temporary-table GROUP BY (13 against 18 ms for
-- 8,300 rows at 1M rows, measured 16 Nov 2026).
SELECT channel, outcome
FROM delivery_attempts FORCE INDEX (attempts_window_idx)
WHERE started_at >= sqlc.arg(from_time) AND started_at < sqlc.arg(until_time);

-- name: MetricsFinishesBetween :many
-- Notifications that reached sent or failed in [from_time, until_time), one
-- row each with its accept-to-sent latency in whole ms (0 for failed), from
-- finished_idx (finished_at, status, channel, created_at, scheduled_at) alone.
-- The latency is the expression of the rollup triggers in
-- schema/notifications.sql. The caller buckets (latencyBucket) and counts
-- them: 23 ms for 8,200 rows against 36 ms grouped by latency and 40 ms
-- bucketed in SQL (1M rows, measured 16 Nov 2026).
SELECT channel, status,
  CAST(CASE WHEN status = 'sent' THEN CEIL(TIMESTAMPDIFF(MICROSECOND,
    GREATEST(COALESCE(scheduled_at, created_at), created_at), finished_at) / 1000) ELSE 0 END AS SIGNED) AS latency_ms
FROM notifications FORCE INDEX (finished_idx)
WHERE finished_at >= sqlc.arg(from_time) AND finished_at < sqlc.arg(until_time)
  AND status IN ('sent', 'failed');
