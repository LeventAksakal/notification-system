# dials-derived: acceptance tests to change (blocked by the agent guard, owner item 0002)

`config.Load` now refuses `APP_LEASE_DURATION` < `APP_PROVIDER_TIMEOUT` + longest slot wait + one slot interval + 500 ms lease margin (`config/cross_checks.go`). The longest slot wait is the send alignment's ceiling (lease/8, at least 200 ms) plus the larger of `APP_RATE_LIMIT_HORIZON` and that ceiling, so a quarter of the lease with the default horizon once the lease is 1.6 s or more (below that the 200 ms floor sets the wait at 400 ms); the slot interval is 10.527 ms at 95/s. With the default horizon the floor solves to lease ≥ (timeout + 510.527 ms) / 0.75: 7.35 s at a 5 s timeout and 3.35 s at 2 s; at 500 ms the floor's 400 ms wait applies instead, so 1.411 s.

Every acceptance fixture was checked against it on the rebased tree (`1d2c4f3`). Two configure lease 2 s with timeout 5 s, so their stack exits at start; the rest pass (`dlFastEnv` 500 ms / 2 s needs 1.51 s; the gaps concurrency test 2 s / 5 s needs 3.76 s; the harness default 5 s / 30 s needs 13.01 s). The test-writer's edits in the lane's worktree were refused by the guard; apply these in the main checkout.

## 1. `test/acceptance/delivery_crash_test.go`, `TestDeliveryR11OnceUnderTimeoutsAndCrashes`, subtest "worker killed while its sends are held"
- Replace the subtest's lease with `"APP_LEASE_DURATION": "8s",` next to `"APP_PROVIDER_TIMEOUT": "5s",` (floor 7.35 s).
- Dials comment: "APP_PROVIDER_TIMEOUT=5s so the 3 s delay holds the sends instead of timing them out; APP_LEASE_DURATION=8s, the smallest whole second config.Load accepts with that timeout; APP_REAP_INTERVAL=500ms, so the crashed worker's claims return after ~8.5 s." Its 30 s wait still covers it.

## 2. `test/acceptance/delivery_gaps_test.go`, `TestGapDeliveryR11CallOutlivesLease`
Configuration can no longer make the lease shorter than a whole call; the cap remains in the code for a worker paused between claim and call and stays unit-tested (`internal/delivery/worker_safety_test.go`). Replace it with `TestGapDeliveryR11SlowCallEndsInsideLease` (R11; decision 0018; contract rule "A request is sent only if its lease outlives the call"):
- Dials: `dlFastEnv` + `APP_PROVIDER_TIMEOUT=2s`, `APP_LEASE_DURATION=3500ms` (floor 3.35 s), `APP_MAX_ATTEMPTS=5`, and a breaker that cannot trip (`APP_BREAKER_FAILURES=10000`; the fixture's `APP_BREAKER_MIN_REQUESTS` no longer exists since decision 0026); provider `WithDedupe(false)`; 2 workers.
- Fault: `Fault{Channel: "sms", Mode: ModeAcceptThenHang, DelayMS: 1500, Count: 6}`: delivery counted on arrival, answered after 1.5 s, inside the timeout. With dedupe off, a cut or reclaimed call would show as a second delivery.
- Assert: 20 sms all `sent` within 30 s; `dgAccounted` rows equal requests; every id has exactly one provider request; `MaxDeliveriesPerKey` is 1; nothing left `processing`; exactly 6 notifications have one attempt with `DurationMs >= 1400` (the hold happened).
- Dropped: the dedupe-on run (adds nothing once each key must have one request) and the 4 s hold past the timeout (the crash test's accept-then-hang subtests cover a hang past the timeout).

## New acceptance test proposed
`TestConfigRefusesALeaseShorterThanAProviderCall` (R-config, decision 0018): `app env check` with `APP_DATABASE_DSN=x APP_LEASE_DURATION=3s APP_PROVIDER_TIMEOUT=5s` exits non-zero and prints one line naming `APP_LEASE_DURATION 3s is below APP_PROVIDER_TIMEOUT 5s`; `app worker` with the same environment exits non-zero before claiming anything. Unit-level coverage exists in `config/cross_checks_test.go`.

## Seen in passing, not this lane's
The acceptance harness and several fixtures still set `APP_BREAKER_WINDOW`, `APP_BREAKER_MIN_REQUESTS` and `APP_BREAKER_FAILURE_RATIO`, which provider-health replaced with `APP_BREAKER_FAILURES` (decision 0026). `config.Load` ignores unknown variables, so they pass silently and the breaker runs at its defaults; `app env check` would report each as an unknown setting.
