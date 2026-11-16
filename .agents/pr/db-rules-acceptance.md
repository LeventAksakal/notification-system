# Acceptance tests held by the guard: db-rules-retention lane

The test-writer could not write `test/acceptance/` from this worktree (agentguard roots paths at the main checkout; owner item 0002). These are the tests to write once the lane has landed. Each is black-box: it runs against the stack started by `startStack`, uses the HTTP API and SQL reads only, and cites its requirement row.

## Row to add to `docs/REQUIREMENTS.md` (the coordinator owns the file)
`| O1 | Operational: storage does not grow forever | Finished notifications (sent, failed, cancelled) and their delivery attempts are deleted once finished longer than APP_NOTIFICATION_RETENTION ago; pending, processing, scheduled and recently finished ones, and templates, stay; a batch older than the retention goes when none of its notifications is left | 0025 | TestO1RetentionDeletesExpiredFinished; internal/store/retention_integration_test.go |`

## 1. `TestO1RetentionDeletesExpiredFinished` (row O1)
Stack env: `APP_NOTIFICATION_RETENTION=1h`, `APP_IDEMPOTENCY_RETENTION=1h`, `APP_IDEMPOTENCY_SWEEP_INTERVAL=1s` (the sweep tick runs retention too).

Setup:
- Create through the API:
  - one notification each that will end sent, failed (provider fault `permanent` for its key), and cancelled (cancel before it is due);
  - one scheduled an hour ahead;
  - a batch of two;
  - a template.
- Wait until the three are final.
- Back-date with SQL, through the state machine, which allows it because the status does not change: `UPDATE notifications SET finished_at = NOW(3) - INTERVAL 2 HOUR, created_at = NOW(3) - INTERVAL 2 HOUR WHERE id IN (...)`. Do the same for both items of the batch once they are sent, plus `UPDATE batches SET created_at = NOW(3) - INTERVAL 2 HOUR`.
- Create one more notification and let it be sent (recent).

Asserts, polled for at most 10 s:
- `GET /notifications/{id}` returns 404 for the three expired notifications and both batch items, and `GET /batches/{id}` returns 404.
- `SELECT COUNT(*) FROM delivery_attempts WHERE notification_id IN (...)` is 0 for the expired ones.
- The scheduled and the recent notification still answer 200, with their attempts.
- The template still answers 200.
- A replay of the expired failed one answers 404.

## 2. `TestR11CrashedCallStaysInTheHistory` (row R11, delivery half; record 0005 "Attempts written before the call")
Stack: `Workers: 0`; env `APP_LEASE_DURATION=2s`, `APP_REAP_INTERVAL=500ms`, `APP_PROVIDER_TIMEOUT=5s`; provider with dedupe on (the default).

Setup:
- Create 10 SMS through the API.
- Set the provider fault `ModeDelay` to 3000 ms.
- `StartWorker`, then wait until the provider received all 10.
- `KillWorker` while the requests are held.
- Clear the faults, then `StartWorker` again.

Asserts:
- All 10 become sent within 30 s.
- For each notification, the attempt history read with SQL (`SELECT attempt_no, outcome FROM delivery_attempts WHERE notification_id = ? ORDER BY attempt_no`) is exactly `[1 unknown, 2 accepted]`.
- The provider stats show `MaxDeliveriesPerKey == 1` and `MaxRequestsPerKey == 2`.
- `GET /notifications/{id}` lists only attempt 2 until owner item 0015 adds `unknown` to the API; after that, both.

This is the black-box twin of `internal/delivery/attempt_integration_test.go` `TestACallWhoseWorkerDiedStaysInTheHistory`, which already passes in-process. The existing `TestDeliveryR11OnceUnderTimeoutsAndCrashes` "worker killed" subtest can carry the history assertion instead of a new test.

## 3. `TestDatabaseRefusesAForbiddenStatusChange` (record 0005, "every rule at every layer"; no requirement row yet)
Optional: the store-level tests already cover every pair (`internal/store/rules_integration_test.go`).

- Against the running stack's database, send `UPDATE notifications SET status = 'sent', finished_at = NOW(3), provider_message_id = 'x', attempts = 1 WHERE id = ?` for a cancelled notification.
- Assert MySQL error 3819 naming `status_transition`.
- Assert the API still reports the notification as cancelled.
