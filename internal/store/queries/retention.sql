-- Retention (decision 0025): finished notifications, their attempts and their
-- batches are deleted once they are older than the retention, on the database
-- clock, in bounded batches.

-- name: RetentionCandidates :many
-- Runs inside a READ COMMITTED transaction: up to LIMIT finished notifications
-- that finished before `before`, oldest first, through finished_idx. The
-- caller sets before to the start of the minute the retention reaches into,
-- the bound the metrics rollups are swept at too: a minute's rows and its
-- rollup go together, so a metrics window (at most the retention) that starts
-- in a minute finds both whole. The rows are locked, so a replay waits for the delete (and then
-- finds nothing) instead of reviving a row whose history is being deleted;
-- rows another sweeper or a replay holds are skipped.
SELECT id FROM notifications
WHERE finished_at < sqlc.arg(before)
  AND status IN ('sent', 'failed', 'cancelled')
ORDER BY finished_at
LIMIT ?
FOR UPDATE SKIP LOCKED;

-- name: DeleteAttemptsOf :execrows
-- The attempts of the rows locked by RetentionCandidates (attempts_notification_idx).
DELETE FROM delivery_attempts WHERE notification_id IN (sqlc.slice(ids));

-- name: DeleteNotifications :execrows
-- The rows locked by RetentionCandidates, after their attempts.
DELETE FROM notifications WHERE id IN (sqlc.slice(ids));

-- name: SweepBatches :execrows
-- Up to LIMIT batches older than the retention none of whose notifications is
-- left (batches_created_idx, then batch_position_idx per batch). A batch's
-- notifications are inserted with it, so an old batch never gains one.
DELETE FROM batches
WHERE created_at < NOW(3) - INTERVAL sqlc.arg(keep_us) MICROSECOND
  AND NOT EXISTS (SELECT 1 FROM notifications WHERE notifications.batch_id = batches.id)
ORDER BY created_at
LIMIT ?;

-- name: SweepAttemptRollups :execrows
-- Up to LIMIT attempts_per_minute rows of minutes before `before` (primary
-- key range), the minute bound RetentionCandidates deletes rows at. Deleting a
-- minute while rows it counts are kept, or keeping it while some are deleted,
-- would make a window that starts in it subtract raw rows that no longer
-- match its rollup (negative or inflated counts). The endpoint reads at most
-- MaxMetricsWindow (1 h), which the retention's 1 h floor covers.
DELETE FROM attempts_per_minute
WHERE minute < sqlc.arg(before)
ORDER BY minute
LIMIT ?;

-- name: SweepFinishRollups :execrows
-- As SweepAttemptRollups, for finishes_per_minute.
DELETE FROM finishes_per_minute
WHERE minute < sqlc.arg(before)
ORDER BY minute
LIMIT ?;

-- name: FoldAttemptRollups :execrows
-- Adds the shards 1 to 3 of attempts_per_minute minutes in [from_minute,
-- until_minute) into shard 0 (DeleteFoldedAttemptShards removes them in the
-- same transaction). Shards only spread the minutes still being written;
-- once folded, a past minute is one row per key again, as cheap to read as
-- unsharded rollups. The locking read keeps the range still until commit.
INSERT INTO attempts_per_minute (minute, channel, outcome, shard, n)
SELECT folded.minute, folded.channel, folded.outcome, 0, folded.total FROM (
  SELECT s.minute, s.channel, s.outcome, SUM(s.n) AS total
  FROM attempts_per_minute AS s
  WHERE s.minute >= sqlc.arg(from_minute) AND s.minute < sqlc.arg(until_minute) AND s.shard > 0
  GROUP BY s.minute, s.channel, s.outcome
) AS folded
ON DUPLICATE KEY UPDATE n = attempts_per_minute.n + folded.total;

-- name: DeleteFoldedAttemptShards :execrows
DELETE FROM attempts_per_minute
WHERE minute >= sqlc.arg(from_minute) AND minute < sqlc.arg(until_minute) AND shard > 0;

-- name: FoldFinishRollups :execrows
-- As FoldAttemptRollups, for finishes_per_minute.
INSERT INTO finishes_per_minute (minute, channel, status, latency_le_ms, shard, n)
SELECT folded.minute, folded.channel, folded.status, folded.latency_le_ms, 0, folded.total FROM (
  SELECT s.minute, s.channel, s.status, s.latency_le_ms, SUM(s.n) AS total
  FROM finishes_per_minute AS s
  WHERE s.minute >= sqlc.arg(from_minute) AND s.minute < sqlc.arg(until_minute) AND s.shard > 0
  GROUP BY s.minute, s.channel, s.status, s.latency_le_ms
) AS folded
ON DUPLICATE KEY UPDATE n = finishes_per_minute.n + folded.total;

-- name: DeleteFoldedFinishShards :execrows
DELETE FROM finishes_per_minute
WHERE minute >= sqlc.arg(from_minute) AND minute < sqlc.arg(until_minute) AND shard > 0;
