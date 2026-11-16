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
-- claim_idx range over the not-yet-due pending rows, reading attempts.
SELECT COUNT(*) AS n
FROM notifications FORCE INDEX (claim_idx)
WHERE status = 'pending' AND due_at > sqlc.arg(now) AND attempts = 0;

-- name: MetricsFinished :many
-- Notifications that reached sent or failed in the window (since its
-- start), per channel, from finished_idx (finished_at, status, channel) alone.
SELECT
  channel,
  CAST(COALESCE(SUM(status = 'sent'), 0) AS SIGNED) AS sent,
  CAST(COALESCE(SUM(status = 'failed'), 0) AS SIGNED) AS failed
FROM notifications FORCE INDEX (finished_idx)
WHERE finished_at >= sqlc.arg(since)
  AND status IN ('sent', 'failed')
GROUP BY channel;

-- name: MetricsAttempts :many
-- Provider calls started in the window (since its start), per channel, from
-- attempts_window_idx (started_at, channel, outcome) alone. attempts are the
-- calls with a result (accepted, transient, permanent), as the API documents
-- them; a call still in flight counts nowhere; unknown (the result was never
-- recorded) is its own count, neither failed nor accepted.
SELECT
  channel,
  CAST(COALESCE(SUM(outcome IN ('accepted', 'transient', 'permanent')), 0) AS SIGNED) AS attempts,
  CAST(COALESCE(SUM(outcome IN ('transient', 'permanent')), 0) AS SIGNED) AS failed_attempts,
  CAST(COALESCE(SUM(outcome = 'unknown'), 0) AS SIGNED) AS unknown_attempts
FROM delivery_attempts FORCE INDEX (attempts_window_idx)
WHERE started_at >= sqlc.arg(since)
GROUP BY channel;
