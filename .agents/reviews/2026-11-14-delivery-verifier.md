# Delivery slice: fresh-context verifier report (14 Nov 2026, Opus, snapshot eb54e0d)

Verdict: **fail**. The 100 per second ceiling held everywhere (busiest second 98 on every channel), but throughput did not: `TestDeliveryR8RateLimit` averaged 89.7 / 89.5 / 86.2 per second against a floor of 95, and the limiter integration test 93.9. The machine was shared with two other lanes and the verifier's own `-race` run at the time; the implementer and the coordinator had each measured 97.1 on a quieter machine. Either way the pace depends on machine load, which is itself a finding.

Proofs: delivery acceptance 109.3 s, only R8 failed; `go test -race ./internal/... ./cmd/...` pass; store integration pass (33.6 s); limiter integration fail (93.9 per second); `go vet` clean.

| # | Finding | Where | Reproduced | Status |
|---|---|---|---|---|
| 1 | Throughput drops under load: a permit that is taken late is lost, and only 4 claims per channel per worker may be in flight | `internal/worker/worker.go:119` | yes, once | queued: delivery fix-up (permits become reserved send slots, one claim query per group) |
| 2 | No lease check before the send: a worker paused past its lease sends anyway, a duplicate call with no attempt row; a lease shorter than the provider timeout is allowed | `worker.go:269-276`, `internal/config/config.go:85,101` | inferred | queued |
| 3 | The 200 ms pacing does not bound late sends; a late send goes out at once on top of the full rate | `worker.go:241,269` | inferred | queued |
| 4 | Breaker results are not tied to the probe: a call admitted while closed can decide the half-open state, and `Cancel` admits a second probe | `internal/breaker/breaker.go:94-130` | inferred | queued |
| 5 | One hung channel stalls the others: all channels share one pool of 32 slots | `worker.go:133,200` | inferred | queued |
| 6 | The reaper's processing to pending change is published to nobody | `worker.go:444` | inferred | queued (port change: `ReapExpired` returns what it reaped) |
| 7 | Headroom 0 is allowed; with the half-interval slack that is 101 per second | `config.go:95`, `internal/limiter/limiter.go:73` | inferred | queued |
| 8 | Weight 0 is accepted, so low can starve | `config.go:173` | inferred | kept: decision 0010 gives the owner exactly that dial |

Owner item 0007: disabling the automatic POST replay, the delivery span as a child of the stored trace, and "only transient outcomes trip the breaker" are sound. The send pacing is not sound as built (findings 1 and 3).

Test gaps: no test pauses a worker past its lease; nothing compares provider requests with `delivery_attempts` rows; R12's 1 s slack cannot tell backoff from pacing; B1's outage is a fast 503, never a hang, and its 25% bound is loose (48 seen, 146 allowed); R8 covers a steady drain only; R16 does not assert the correlation and trace headers (R14 and B5 do).
