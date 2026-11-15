-- Intake: create, read, list, cancel and replay notifications; batches;
-- idempotency keys; templates. The API status `scheduled` is computed on the
-- database clock (stored pending, never tried, due in the future), the same
-- rule the list filter uses.

-- name: InsertNotification :exec
-- due_at is the schedule or now on the database clock; a past schedule means now.
INSERT INTO notifications (
  id, recipient, channel, priority, content, due_at, scheduled_at, correlation_id, traceparent, created_at
) VALUES (
  sqlc.arg(id), sqlc.arg(recipient), sqlc.arg(channel), sqlc.arg(priority), sqlc.arg(content),
  GREATEST(COALESCE(sqlc.narg(scheduled_at), NOW(3)), NOW(3)),
  sqlc.narg(scheduled_at), sqlc.arg(correlation_id), sqlc.narg(traceparent), sqlc.arg(created_at)
);

-- name: InsertBatch :exec
INSERT INTO batches (id, total, correlation_id, created_at) VALUES (?, ?, ?, ?);

-- name: GetNotification :one
SELECT sqlc.embed(notifications),
  CAST(notifications.status = 'pending' AND notifications.attempts = 0 AND notifications.due_at > NOW(3) AS SIGNED) AS is_scheduled
FROM notifications
WHERE id = ?;

-- name: ListDeliveryAttempts :many
-- Oldest first (attempts_notification_idx).
SELECT * FROM delivery_attempts WHERE notification_id = ? ORDER BY id;

-- ListNotifications is built in Go (store.ListNotificationsQuery): the WHERE
-- holds only the filters a request sets, so each filter keeps its index.

-- name: CancelNotification :execrows
-- The only way out of pending to cancelled; a lost race changes nothing.
UPDATE notifications
SET status = 'cancelled', finished_at = NOW(3)
WHERE id = ? AND status = 'pending';

-- name: ReplayNotification :execrows
-- A dead letter goes back to pending with a fresh attempt budget; history is kept.
UPDATE notifications
SET status = 'pending', attempts = 0, budget_spent = 0, failure_kind = NULL, finished_at = NULL, due_at = NOW(3)
WHERE id = ? AND status = 'failed';

-- name: GetBatch :one
SELECT * FROM batches WHERE id = ?;

-- name: CountBatchByStatus :many
-- Counts per API status (scheduled is a view of pending).
SELECT
  CAST(CASE WHEN status = 'pending' AND attempts = 0 AND due_at > NOW(3) THEN 'scheduled' ELSE status END AS CHAR) AS api_status,
  COUNT(*) AS n
FROM notifications
WHERE batch_id = ?
GROUP BY api_status;

-- name: ListBatchItems :many
-- One page of a batch in request order (batch_idx); after_index -1 starts at the first item.
SELECT sqlc.embed(notifications),
  CAST(notifications.status = 'pending' AND notifications.attempts = 0 AND notifications.due_at > NOW(3) AS SIGNED) AS is_scheduled
FROM notifications
WHERE batch_id = sqlc.arg(batch_id) AND CAST(batch_index AS SIGNED) > sqlc.arg(after_index)
ORDER BY batch_index
LIMIT ?;

-- name: ListBatchNotificationIDs :many
-- Every id of a batch in request order; rebuilds a replayed BatchCreated.
SELECT id FROM notifications WHERE batch_id = ? ORDER BY batch_index;

-- name: InsertIdempotencyKey :exec
INSERT INTO idempotency_keys (idem_key, request_hash, resource_kind, resource_id) VALUES (?, ?, ?, ?);

-- name: GetIdempotencyKey :one
SELECT * FROM idempotency_keys WHERE idem_key = ?;

-- name: DeleteExpiredIdempotencyKey :execrows
-- Forget one key older than the retention, so it can be used again.
DELETE FROM idempotency_keys WHERE idem_key = ? AND created_at < ?;

-- name: SweepIdempotencyKeys :execrows
-- Forget up to 1000 keys older than the retention, on the database clock (the
-- clock that set created_at), through idempotency_created_idx.
DELETE FROM idempotency_keys
WHERE created_at < NOW(3) - INTERVAL sqlc.arg(retention_ms) * 1000 MICROSECOND
LIMIT 1000;

-- name: UpsertTemplate :exec
INSERT INTO templates (name, body) VALUES (?, ?)
ON DUPLICATE KEY UPDATE body = VALUES(body);

-- name: GetTemplate :one
SELECT * FROM templates WHERE name = ?;

-- name: ListTemplates :many
SELECT * FROM templates ORDER BY name;

-- name: DeleteTemplate :execrows
DELETE FROM templates WHERE name = ?;
