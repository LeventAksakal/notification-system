# Provider health: acceptance tests held for owner item 0002

The test-writer drafted these on 15 Nov 2026. They are not written, because agentguard denies the test-writer in a worktree. The coordinator has the test-writer write them in the main checkout once this lane lands. Rows: B1 (provider outage) and R12 (retry and dead letter). Rule: decision 0026.

## New tests

### Long outage, one probe per cool-down fleet-wide (B1): `TestDeliveryB1SharedHealthLongOutage`
- **Dials:** `APP_BREAKER_FAILURES=5`, `APP_BREAKER_COOLDOWN=1s`, `APP_RETRY_BASE=100ms`, `APP_RETRY_CAP=1s`, `APP_MAX_ATTEMPTS=5`.
- **Load:** 3 workers. Queue 300 sms, 600 email and 600 push, plus one sms every 500 ms during a 12 s sms outage (503). The outage is 5 retry spans of 0.2 + 0.4 + 0.8 + 1.0 = 2.4 s, and 12 cool-downs.
- **Asserts:**
  - No notification is `failed` during the outage, and all are `sent` after it.
  - `SUM(budget_spent)` ≤ 4 (K-1).
  - After a 1.5 s settle, sms requests during the outage ≤ ⌊window / 1 s⌋ + 1. The settle is 53 ms detection + 200 ms horizon + 200 ms send alignment + 500 ms timeout + 250 ms idle poll, rounded up.
  - The first accepted sms comes ≤ 2 s after recovery (1 s cool-down plus 1 s slack).
  - The sms busiest sliding second is ≤ 100.
  - Email and push keep at least the suite's rate floor (88/s) during the sms outage.

### A message-specific failure is still dead-lettered (R12): `TestDeliveryR12MessageSpecificFailureDeadLetters`
- **Setup:** a fake-provider `Fault` with `To` set to one recipient, which always gets 503. An sms stream to other recipients every 50 ms. `APP_MAX_ATTEMPTS=10`.
- **Asserts:** that notification ends `failed` with `failure_kind = exhausted` and `budget_spent = 10`. Every other notification is `sent`.

### A lone message outlives an outage (B1): `TestDeliveryB1LoneMessageOutlivesOutage`
- **Setup:** the long-outage dials, with `APP_MAX_ATTEMPTS=5` (= K, the edge case). One sms, a 12 s outage.
- **Asserts:**
  - It is never `failed`.
  - It is `sent` within 2 s of recovery.
  - `budget_spent` ≤ 4.
  - `attempts` equals its number of `delivery_attempts` rows.

### Measurement
The draft's measurement test is replaced by `internal/delivery/provider_health_measure_test.go` (tag `integration`, run with `MEASURE_PROVIDER_HEALTH=1`). The PR notes hold its results.

## Existing tests that need the new dial names
`.agents/pr/provider-health-acceptance-env.patch` was made against main (`ebedafb`). At integration `d580c4e` it applies cleanly to two files. The other three fail on context only: the generated client renamed fields (for example `n.ID` became `n.Id.String()`). The dial changes below are what the patch makes. Until they land, these tests set dials that `config.Load` no longer reads (it ignores unknown names), so the default K = 5 applies.

| File | Change | Why |
|---|---|---|
| `harness_test.go` (default env) | Remove `APP_BREAKER_WINDOW`, `_MIN_REQUESTS` and `_FAILURE_RATIO`. Add `"APP_BREAKER_FAILURES": "5"` | The service default |
| `observe_metrics_test.go` | `APP_BREAKER_MIN_REQUESTS=1000` → `APP_BREAKER_FAILURES=1000` | Keeps health out of the test |
| `delivery_crash_test.go` (R11, first stack) | `APP_BREAKER_MIN_REQUESTS=1000` → `APP_BREAKER_FAILURES=1000` | Timeouts must not declare an outage. With no outage every failure counts, so calls per key stay ≤ `APP_MAX_ATTEMPTS` |
| `delivery_gaps_test.go`: lease-held test, 30 % 503 test | `APP_BREAKER_MIN_REQUESTS=1000` → `APP_BREAKER_FAILURES=1000` | Same reason. A random run of 503s must not change the pace |
| `delivery_gaps_test.go`: hung-channel test | `APP_BREAKER_MIN_REQUESTS=10000` → `APP_BREAKER_FAILURES=10000` | The dial's upper bound: never declared |
| `delivery_gaps_test.go`: `dgB1Env` | Window, min-requests and ratio → `APP_BREAKER_FAILURES=5`, keep `APP_BREAKER_COOLDOWN=1s` | The outage tests. Probe bound becomes 6 + 1 = 7 fleet-wide (was 3 × 7 = 21, one breaker per process) |
| `delivery_gaps_test.go`: `BreakerBurnsNoAttempts` (line ~395) | Assert `budget_spent > APP_MAX_ATTEMPTS` is 0, not `attempts > APP_MAX_ATTEMPTS` | `APP_MAX_ATTEMPTS` now bounds the budget. Excused calls (in flight at the declaration, probes) are still calls |
| `delivery_retry_test.go`: `TestDeliveryR12Retry` | Set `APP_BREAKER_FAILURES=5` explicitly. Fail fast unless `APP_MAX_ATTEMPTS` (3) < 5. In "always 503", also assert `budget_spent = 3` | A lone message that always fails is otherwise an outage by design: its K-th fault declares it and is excused, and it stays pending |
| `delivery_retry_test.go`: the outage test (line ~189) | Window and min-requests → `APP_BREAKER_FAILURES=5`, keep cool-down 1 s and `APP_MAX_ATTEMPTS=3` | Same rule |
