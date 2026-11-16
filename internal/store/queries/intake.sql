-- Intake: create, read, list, cancel and replay notifications; batches;
-- idempotency keys; templates. The API status `scheduled` is computed on the
-- database clock (stored pending, never tried, due in the future), the same
-- rule the list filter uses.

-- name: InsertNotification :exec
-- due_at is the schedule or now on the database clock; a past schedule means now.
-- template_name and template_version: the template that rendered content, both NULL for plain content.
INSERT INTO notifications (
  id, recipient, channel, priority, content, due_at, scheduled_at, correlation_id, traceparent, created_at,
  template_name, template_version
) VALUES (
  sqlc.arg(id), sqlc.arg(recipient), sqlc.arg(channel), sqlc.arg(priority), sqlc.arg(content),
  GREATEST(COALESCE(sqlc.narg(scheduled_at), NOW(3)), NOW(3)),
  sqlc.narg(scheduled_at), sqlc.arg(correlation_id), sqlc.narg(traceparent), sqlc.arg(created_at),
  sqlc.narg(template_name), sqlc.narg(template_version)
);

-- name: DatabaseNow :one
-- The database clock, the one clock every schedule decision uses (decision
-- 0016): due_at, the claim, the scheduled status and the horizon check.
SELECT CAST(NOW(3) AS DATETIME(3)) AS now;

-- name: RescheduleNotification :execrows
-- A new schedule for a notification no worker has claimed yet (claim_version
-- 0: never leased, so no attempt, retry backoff or Retry-After is pending).
-- One statement: a claim racing it on the same row either leased it first
-- (no row matches) or leases it after with the new due_at. due_at follows
-- the create rule (a past or absent schedule means now). When the row
-- changes, ON UPDATE stamps updated_at with this statement's NOW(3), which the
-- scheduled_within_horizon CHECK measures from; when it does not (the same
-- schedule again), nothing is written and 0 rows are reported.
UPDATE notifications
SET scheduled_at = sqlc.narg(scheduled_at),
    due_at = GREATEST(COALESCE(sqlc.narg(scheduled_at), NOW(3)), NOW(3))
WHERE id = sqlc.arg(id) AND status = 'pending' AND claim_version = 0;

-- name: InsertBatch :exec
INSERT INTO batches (id, total, correlation_id, created_at) VALUES (?, ?, ?, ?);

-- name: GetNotification :one
SELECT sqlc.embed(notifications),
  CAST(notifications.status = 'pending' AND notifications.attempts = 0 AND notifications.due_at > NOW(3) AS SIGNED) AS is_scheduled
FROM notifications
WHERE id = ?;

-- name: ListDeliveryAttempts :many
-- Oldest first (attempts_notification_idx). Only calls with a result: the API
-- contract's outcome enum has no in_flight or unknown (owner item 0015); an
-- unknown call shows as the gap in attempt_no.
SELECT * FROM delivery_attempts WHERE notification_id = ? AND outcome IN ('accepted', 'transient', 'permanent') ORDER BY id;

-- ListNotifications is built in Go (store.ListNotificationsQuery): the WHERE
-- holds only the filters a request sets, so each filter keeps its index.

-- name: CancelNotification :execresult
-- The only way out of pending to cancelled; a lost race changes nothing.
-- The trigger stores status_seq + 1; LAST_INSERT_ID(expr) computes the same
-- value from the old row and hands it back in the OK packet
-- (sql.Result.LastInsertId), so the event carries it without a second read
-- that a later change could overtake.
UPDATE notifications
SET status = 'cancelled', status_seq = LAST_INSERT_ID(status_seq + 1), finished_at = NOW(3)
WHERE id = ? AND status = 'pending';

-- name: ReplayNotification :execresult
-- A dead letter goes back to pending with a fresh attempt budget; history is
-- kept. The new status_seq comes back as LastInsertId, as for a cancel.
UPDATE notifications
SET status = 'pending', status_seq = LAST_INSERT_ID(status_seq + 1), attempts = 0, budget_spent = 0, failure_kind = NULL, finished_at = NULL, due_at = NOW(3)
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
-- One page of a batch in request order (batch_position_idx); after_index -1 starts at the first item.
SELECT sqlc.embed(notifications),
  CAST(notifications.status = 'pending' AND notifications.attempts = 0 AND notifications.due_at > NOW(3) AS SIGNED) AS is_scheduled
FROM notifications
WHERE batch_id = sqlc.arg(batch_id) AND CAST(batch_index AS SIGNED) > sqlc.arg(after_index)
ORDER BY batch_index
LIMIT ?;

-- name: ListBatchStatusSeqs :many
-- Every notification of a batch with its status_seq (batch_idx, then the
-- row): the base a batch subscription orders live events against.
SELECT id, status_seq FROM notifications WHERE batch_id = ?;

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

-- name: InsertTemplate :exec
-- Creates version 1 (the insert trigger sets it); a duplicate key means the
-- template exists and an update needs If-Match (decision 0015).
INSERT INTO templates (name, body) VALUES (?, ?);

-- name: UpdateTemplate :execrows
-- Optimistic concurrency: replaces the body only if the caller read the
-- current version; 0 rows means stale or absent. version = version + 1 is what
-- the update trigger forces anyway; it is written here so the query reads true.
UPDATE templates SET body = sqlc.arg(body), version = version + 1
WHERE name = sqlc.arg(name) AND version = sqlc.arg(version);

-- name: GetTemplate :one
SELECT * FROM templates WHERE name = ?;

-- name: ListTemplates :many
SELECT * FROM templates ORDER BY name;

-- name: DeleteTemplate :execrows
DELETE FROM templates WHERE name = ?;
