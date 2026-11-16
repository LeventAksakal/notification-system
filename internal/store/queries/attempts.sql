-- Attempts are written before the provider call (decision 0005): in flight,
-- committed with the claim, then given their result. One claim makes at most
-- one call (attempts_claim_idx).

-- name: BeginAttempts :execrows
-- Runs in the claim's transaction, after LeaseNotifications: each claimed row's
-- call, in flight, numbered from the notification's count and carrying the new
-- claim_version. One statement per claim, however many rows; the claim's row
-- locks are held, so no reaper or release can interleave.
INSERT INTO delivery_attempts (notification_id, attempt_no, channel, started_at, outcome, claim_version)
SELECT n.id, n.attempts + 1, n.channel, NOW(3), 'in_flight', n.claim_version
FROM notifications n
WHERE n.id IN (sqlc.slice(ids)) AND n.status = 'processing';

-- name: FinishAttempt :execrows
-- started_at stays the claim's time (at most the reservation horizon before
-- the request): rewriting it would move the row in attempts_window_idx a
-- second time on every send.
-- The result of the claim's call (attempts_claim_idx); a result may replace
-- unknown when it arrives after the lease ran out.
UPDATE delivery_attempts
SET outcome = sqlc.arg(outcome), duration_ms = sqlc.arg(duration_ms), http_status = sqlc.narg(http_status),
    error = sqlc.narg(error), provider_message_id = sqlc.narg(provider_message_id)
WHERE notification_id = sqlc.arg(notification_id) AND claim_version = sqlc.arg(claim_version)
  AND outcome IN ('in_flight', 'unknown');

-- name: InFlightOf :many
-- Which of the reaper's locked rows have an attempt in flight on their current claim.
SELECT a.notification_id
FROM delivery_attempts a
JOIN notifications n ON n.id = a.notification_id AND n.claim_version = a.claim_version
WHERE a.notification_id IN (sqlc.slice(ids)) AND a.outcome = 'in_flight';

-- name: UnknownOnReap :execrows
-- The reaper's rows: an attempt in flight on an expired lease becomes unknown
-- and counts toward the notification's attempts (the request may have left)
-- and against its budget: the reaper cannot tell an outage from a message
-- that kills its worker, and the second must end (decision 0026).
UPDATE notifications n
JOIN delivery_attempts a ON a.notification_id = n.id AND a.claim_version = n.claim_version
SET a.outcome = 'unknown', a.error = 'the lease ran out before a result was recorded', n.attempts = n.attempts + 1,
    n.budget_spent = n.budget_spent + 1
WHERE n.id IN (sqlc.slice(ids)) AND n.status = 'processing' AND a.outcome = 'in_flight';

-- name: DeleteUnsentAttempt :execrows
-- The claim's call was never made (a release: the job missed its slot, the
-- an outage was declared, shutdown came first; or FailSpent), so its in-flight
-- attempt is removed, not counted. Runs after the notification's update in
-- the same transaction.
DELETE FROM delivery_attempts
WHERE notification_id = sqlc.arg(notification_id) AND claim_version = sqlc.arg(claim_version) AND outcome = 'in_flight';

-- name: FailSpent :execrows
-- processing -> failed without a call: unknown calls (counted by the reaper)
-- have already spent the attempt budget. Fenced by claim_version like the
-- other finishes, so the new status_seq is the claim's plus one.
UPDATE notifications
SET status = 'failed', last_error = sqlc.narg(last_error), failure_kind = 'exhausted', lease_until = NULL, finished_at = NOW(3)
WHERE id = sqlc.arg(id) AND status = 'processing' AND claim_version = sqlc.arg(claim_version);
