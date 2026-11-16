-- Retention (decision 0025): finished notifications, their attempts and their
-- batches are deleted once they are older than the retention, on the database
-- clock, in bounded batches.

-- name: RetentionCandidates :many
-- Runs inside a READ COMMITTED transaction: up to LIMIT finished notifications
-- past the retention, oldest first, through finished_idx (finished_at, status,
-- channel). The rows are locked, so a replay waits for the delete (and then
-- finds nothing) instead of reviving a row whose history is being deleted;
-- rows another sweeper or a replay holds are skipped.
SELECT id FROM notifications
WHERE finished_at < NOW(3) - INTERVAL sqlc.arg(keep_us) MICROSECOND
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
