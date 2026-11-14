# QA branch: fresh-context verifier report (14 Nov 2026, Opus, on `fix/ci-rate-floors-qa`)

Written to this file by the coordinator from the verifier's hand-back: the guard refuses redirects for the verifier role and it has no write tool.

Verdict: **pass with findings**. None breaks the 100 per second cap. Two are liveness or contract defects, one is a test-coverage gap.

| # | Severity | Finding | Where | Reproduced | Status |
|---|---|---|---|---|---|
| 1 | Medium | A worker whose every limiter round trip in the last 10 s took a whole interval or more never sends, and keeps reserving one real slot every 50 ms to learn the clock: up to 20 slots per second per channel taken from the workers that can send | `internal/worker/worker.go` (uncertain path), `internal/limiter/limiter.go` (n = 0 never reaches Valkey) | reasoned | see "After the report" |
| 2 | Medium | No read or write timeout in the DSN and no request deadline: if MySQL hangs (paused container, black-holed network) instead of refusing, the API hangs instead of answering 503. `TestR15APIUnavailableWhileMySQLDown` covers refused connections only | `internal/store/store.go`, `internal/store/errors.go` | reasoned; the compose proof was refused by the guard | see "After the report" |
| 3 | Medium | The 503 body says "nothing was stored", but a connection error can come from a commit the server did apply; a client retrying without an `Idempotency-Key` then creates a duplicate | `internal/api/handlers.go`, `internal/store/errors.go` | reasoned | see "After the report" |
| 4 | Low | CI sets `ACCEPTANCE_RATE_FLOOR=40` and the worker package's own rate test now asserts 40: CI cannot catch a throughput regression that stays above 40 per second. Caps and counts are untouched; the knob only lowers floors; locally the floors stay strict | `.github/workflows/ci.yml`, `internal/worker/worker_integration_test.go` | reasoned | kept; owner item 0011 |
| 5 | Low | The isolation floor went from 85 to 40 in the new no-breaker test: a full stall (17 to 19 per second) is caught, a partial one between 40 and 85 is not. The fast-503 case is still held at 90 by `TestDeliveryB1BreakerPausesChannel` where the floors are strict | `test/acceptance/delivery_gaps_test.go` | reasoned | kept; owner item 0011 |
| 6 | Low | Clock bounds are intersected over 10 s as if the two clocks did not drift: if Valkey's clock runs slow or steps back by less than the uncertainty, a slot can go out early by up to the drift or the step. About two intervals of margin remain | `internal/worker/clock.go` | reasoned | see "After the report" |

Proofs, each once, on a clean tree:
- `go vet ./...`: pass.
- `go test -race ./...`: pass, about 1 minute; several packages came from the test cache.
- `go test -race -tags integration ./internal/...`: pass, about 3 minutes (store 137 s, intake 32 s); worker and limiter came from the cache.
- `go test -tags integration ./test/acceptance -count=1`: pass in 260.3 s with `ACCEPTANCE_RATE_FLOOR` unset, so the strict 90, 93 and 95 floors applied.

Checked, no defect: a late reply cannot place a slot early, and mapping lateness plus tolerance stay within one interval; a grant with N = 0 takes no slot; the probe ticket is cancelled on the uncertain path; the sample store is bounded by time and guarded by its mutex; a Valkey clock step larger than the uncertainty crosses the bounds and restarts the mapping; the log value handler guards a nil Stringer and leaves errors alone; metric labels come from closed sets and cost nothing without a collector; new goroutines are owned through `inflight`; product commits do not touch `test/acceptance/`; parameter errors leak no SQL, DSN or Go type name.

Not checked: the compose stack (the guard refused the environment-variable prefix needed to move the ports, and writing a k6 script), so no Valkey restart under load, worker kill or MySQL hang; k6 thresholds; clock drift or WSL2 time steps measured for real. Test authorship by the test-writer role cannot be confirmed from git: one author, the same trailer.
