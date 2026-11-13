-- The delivery queue (decisions 0003, 0018). A claim is a lease: it sets
-- lease_until and bumps claim_version; every later write by the claimer
-- carries WHERE status = 'processing' AND claim_version = ?, so a worker that
-- lost its lease changes nothing. Times are on the database clock.

-- name: ClaimCandidates :many
-- Runs inside a READ COMMITTED transaction. claim_idx (status, channel,
-- priority, due_at) serves both the filter and the order, so InnoDB locks
-- only the rows it returns; rows locked by another claimer are skipped.
SELECT id, batch_id, recipient, content, attempts, claim_version, due_at, correlation_id, traceparent
FROM notifications
WHERE status = 'pending' AND channel = sqlc.arg(channel) AND priority = sqlc.arg(priority) AND due_at <= NOW(3)
ORDER BY due_at
LIMIT ?
FOR UPDATE SKIP LOCKED;

-- name: LeaseNotification :execrows
-- pending -> processing for a row locked by ClaimCandidates in the same transaction.
UPDATE notifications
SET status = 'processing',
    lease_until = NOW(3) + INTERVAL sqlc.arg(lease_us) MICROSECOND,
    claim_version = claim_version + 1
WHERE id = sqlc.arg(id) AND status = 'pending';

-- name: FinishSent :execrows
-- processing -> sent.
UPDATE notifications
SET status = 'sent', attempts = attempts + 1, provider_message_id = sqlc.narg(provider_message_id),
    lease_until = NULL, finished_at = NOW(3)
WHERE id = sqlc.arg(id) AND status = 'processing' AND claim_version = sqlc.arg(claim_version);

-- name: FinishRetry :execrows
-- processing -> pending, due again after delay_us (a retry is a stored due time, decision 0007).
UPDATE notifications
SET status = 'pending', attempts = attempts + 1, last_error = sqlc.narg(last_error),
    due_at = NOW(3) + INTERVAL sqlc.arg(delay_us) MICROSECOND, lease_until = NULL
WHERE id = sqlc.arg(id) AND status = 'processing' AND claim_version = sqlc.arg(claim_version);

-- name: FinishFailed :execrows
-- processing -> failed (permanent refusal or an exhausted budget).
UPDATE notifications
SET status = 'failed', attempts = attempts + 1, last_error = sqlc.narg(last_error),
    failure_kind = sqlc.arg(failure_kind), lease_until = NULL, finished_at = NOW(3)
WHERE id = sqlc.arg(id) AND status = 'processing' AND claim_version = sqlc.arg(claim_version);

-- name: ReleaseLease :execrows
-- processing -> pending without an attempt (breaker open, shutdown; decision 0008).
UPDATE notifications
SET status = 'pending', due_at = NOW(3) + INTERVAL sqlc.arg(delay_us) MICROSECOND, lease_until = NULL
WHERE id = sqlc.arg(id) AND status = 'processing' AND claim_version = sqlc.arg(claim_version);

-- name: ReapExpiredLeases :execrows
-- processing -> pending for leases that ran out (lease_idx). Bounded per tick.
UPDATE notifications
SET status = 'pending', lease_until = NULL
WHERE status = 'processing' AND lease_until < NOW(3)
LIMIT 1000;

-- name: InsertDeliveryAttempt :exec
INSERT INTO delivery_attempts (
  notification_id, attempt_no, channel, started_at, duration_ms, outcome, http_status, error, provider_message_id
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
