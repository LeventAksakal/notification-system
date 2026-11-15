-- The delivery queue (decisions 0003, 0018). A claim is a lease: it sets
-- lease_until and bumps claim_version; every later write by the claimer
-- carries WHERE status = 'processing' AND claim_version = ?, so a worker that
-- lost its lease changes nothing. Times are on the database clock.

-- name: ClaimCandidates :many
-- Runs inside a READ COMMITTED transaction. claim_idx (status, channel,
-- priority, due_at) serves both the filter and the order, so InnoDB locks
-- only the rows it returns; rows locked by another claimer are skipped.
SELECT id, batch_id, recipient, content, attempts, budget_spent, claim_version, due_at, correlation_id, traceparent
FROM notifications
WHERE status = 'pending' AND channel = sqlc.arg(channel) AND priority = sqlc.arg(priority) AND due_at <= NOW(3)
ORDER BY due_at
LIMIT ?
FOR UPDATE SKIP LOCKED;

-- name: NextDue :many
-- How long until each priority's earliest pending notification of a channel
-- is due, in microseconds on the database's clock (<= 0: due now), and
-- whether it has any pending at all. An idle worker asks this instead of
-- claiming. Each arm is MIN over a constant prefix of claim_idx (status,
-- channel, priority, due_at): one index dive ("Select tables optimized
-- away"), whatever the backlog. A GROUP BY priority form reads every pending
-- entry of the channel instead.
SELECT 'high' AS priority, MIN(h.due_at) IS NOT NULL AS pending, CAST(COALESCE(TIMESTAMPDIFF(MICROSECOND, NOW(3), MIN(h.due_at)), 0) AS SIGNED) AS wait_us
FROM notifications h WHERE h.status = 'pending' AND h.channel = sqlc.arg(channel) AND h.priority = 'high'
UNION ALL
SELECT 'normal', MIN(n.due_at) IS NOT NULL, CAST(COALESCE(TIMESTAMPDIFF(MICROSECOND, NOW(3), MIN(n.due_at)), 0) AS SIGNED)
FROM notifications n WHERE n.status = 'pending' AND n.channel = sqlc.arg(channel) AND n.priority = 'normal'
UNION ALL
SELECT 'low', MIN(l.due_at) IS NOT NULL, CAST(COALESCE(TIMESTAMPDIFF(MICROSECOND, NOW(3), MIN(l.due_at)), 0) AS SIGNED)
FROM notifications l WHERE l.status = 'pending' AND l.channel = sqlc.arg(channel) AND l.priority = 'low';

-- name: LeaseNotifications :execrows
-- pending -> processing for the rows locked by ClaimCandidates in the same
-- transaction, in one statement (one round trip, however many rows).
UPDATE notifications
SET status = 'processing',
    lease_until = NOW(3) + INTERVAL sqlc.arg(lease_us) MICROSECOND,
    claim_version = claim_version + 1
WHERE id IN (sqlc.slice(ids)) AND status = 'pending';

-- name: FinishSent :execrows
-- processing -> sent.
UPDATE notifications
SET status = 'sent', attempts = attempts + 1, provider_message_id = sqlc.narg(provider_message_id),
    lease_until = NULL, finished_at = NOW(3)
WHERE id = sqlc.arg(id) AND status = 'processing' AND claim_version = sqlc.arg(claim_version);

-- name: FinishRetry :execrows
-- processing -> pending, due again after delay_us (a retry is a stored due time, decision 0007).
-- counted is 1 when the failure counts against the budget, 0 when it belongs
-- to a declared provider outage (decision 0026).
UPDATE notifications
SET status = 'pending', attempts = attempts + 1, budget_spent = budget_spent + sqlc.arg(counted), last_error = sqlc.narg(last_error),
    due_at = NOW(3) + INTERVAL sqlc.arg(delay_us) MICROSECOND, lease_until = NULL
WHERE id = sqlc.arg(id) AND status = 'processing' AND claim_version = sqlc.arg(claim_version);

-- name: FinishFailed :execrows
-- processing -> failed (permanent refusal or an exhausted budget); both count.
UPDATE notifications
SET status = 'failed', attempts = attempts + 1, budget_spent = budget_spent + 1, last_error = sqlc.narg(last_error),
    failure_kind = sqlc.arg(failure_kind), lease_until = NULL, finished_at = NOW(3)
WHERE id = sqlc.arg(id) AND status = 'processing' AND claim_version = sqlc.arg(claim_version);

-- name: ReleaseLease :execrows
-- processing -> pending without an attempt (provider outage declared, shutdown; decision 0026).
UPDATE notifications
SET status = 'pending', due_at = NOW(3) + INTERVAL sqlc.arg(delay_us) MICROSECOND, lease_until = NULL
WHERE id = sqlc.arg(id) AND status = 'processing' AND claim_version = sqlc.arg(claim_version);

-- name: ReapCandidates :many
-- Runs inside a READ COMMITTED transaction: the expired leases (lease_idx),
-- locked so two reapers never return the same row. Bounded per tick.
SELECT id, batch_id, channel, attempts, correlation_id
FROM notifications
WHERE status = 'processing' AND lease_until < NOW(3)
ORDER BY lease_until
LIMIT 1000
FOR UPDATE SKIP LOCKED;

-- name: ReapLeases :execrows
-- processing -> pending for rows locked by ReapCandidates in the same transaction.
UPDATE notifications
SET status = 'pending', lease_until = NULL
WHERE id IN (sqlc.slice(ids)) AND status = 'processing';

-- name: InsertDeliveryAttempt :exec
INSERT INTO delivery_attempts (
  notification_id, attempt_no, channel, started_at, duration_ms, outcome, http_status, error, provider_message_id
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
