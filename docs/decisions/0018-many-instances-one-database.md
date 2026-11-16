---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when one MySQL primary can no longer hold the write load
---
# Built for many instances on one database: claims are leases with a fencing token (0018)

## Question
The brief asks for millions of notifications a day with reliable delivery: how do many workers share the work safely when any of them can crash, pause or restart while holding messages?

## Rule
A claim sets `lease_until` and increments `claim_version`. Every later write by that worker carries `WHERE claim_version = ?`; zero rows affected means the lease was lost and the result is discarded.

## Context
The brief: "millions of notifications daily", "high throughput", "reliable delivery". Workers crash, get paused and get restarted while holding work.

## Options
1. Assume one worker. No leases; a crash strands its rows in `processing` for ever, and a second worker is unsafe.
2. Many stateless instances on one MySQL. A claim is a lease: a reaper returns rows whose lease expired to `pending`; the version check stops a worker that was paused past its lease from overwriting the new owner's result. Shared limits live in Valkey (0009). No lock service: the row being worked on is the lock.
3. Option 2 plus a shard resolver and date-partitioned tables now. Ready for data beyond one node; code paths that nothing in this repository can exercise or test.

## Decision
Option 2 (accepted 15 Nov 2026), with option 3 written up in the README as the next step (shard by recipient hash, partition by day, drop old partitions). A lease that expires during a provider call can produce a second send; 0006 covers that case with the idempotency key.

## Confirmation
Fault tests: kill a worker mid-send, and pause a worker container past its lease then resume it. Assert every notification ends `sent`, none stays `processing`, and the paused worker's late write changed nothing.

## Owner review (15 Nov 2026)
- Accepted. One primary is enough: the provider ceiling is 3 channels × 95/s = 285/s (24.6 M a day), at about 5 writes per notification, about 1,400 writes/s. The rate limit binds before the database does.
- Every dial (lease, reap interval, the lease cut, worker concurrency, pool size, and every other runtime setting) gets a derivation: Little's law for concurrency, measured write times for the pool and the cut, lease ≥ provider timeout + horizon + margin. `config.Load` refuses impossible combinations, such as a lease shorter than the provider timeout. (As built, the lease rule also counts the longest slot wait, one slot interval and a 500 ms margin; see the table under "Refused combinations".)
- Next step at scale, written up and not built: daily partitions, so retention is `DROP PARTITION` instead of `DELETE`. The primary key would become `(id, created_at)`.
- Built in session dials-derived (branch `feat-derived-dials`), merged with the rest.

## As built: the numbers (16 Nov 2026)
Every dial and every constant on a worker's claim, lease and send path states where its value comes from: a stated target and its arithmetic, a measurement, or a named default. Free choices say so, and say how to tune them. Each dial's doc line ends with a "Default: ..." clause (`app env` prints it), and a unit test fails a dial without one. `config.Load` refuses the combinations that cannot work (`config/cross_checks.go`, second table); `app env check` reports them.

### Capacity
- Ceiling: 100 per second per channel at the provider, paced 5% under (owner item 0013): 95/s × 3 channels = **285 sends/s**, 24.6 M a day.
- About 5 MySQL statements per notification: create (1), claim (1 per group of up to ~10 rows), attempt row and status in one transaction (2), that transaction's per-minute metrics rollup upserts (metrics-rollup), plus its share of retries, reaps and releases; counted items come to about 3.1 a send before the rollups, rounded up to 5. So **~1,425 writes/s** at the ceiling, on one primary. Status events go to Valkey, not MySQL.
- Provider latency was not measured (no real provider). Assumed **p99 = 500 ms**: vendor SMS, email and push APIs usually answer in 100 to 500 ms. Every derivation that uses it says so. The delivery call-duration histogram is how to replace it with a measurement.

### Numbers
| Dial or constant | Before → now | Source or derivation |
|---|---|---|
| `APP_WORKER_CONCURRENCY` | 32 → **72** | Little's law: calls in flight = rate × latency. Target: 2 of 3 workers carry the full 285/s at the assumed 500 ms p99, so 285 × 0.5 / 2 = 72 each. At 32, the same two workers carry at most 64 / 0.5 = 128/s. A call place is held only for the provider call, not for the database writes. |
| `APP_DB_MAX_OPEN_CONNS` | 10 → 10 | A worker needs 3 claims in flight (one per channel) + 1 reap + result writes. On a healthy disk the writes take 142.5/s × 5 ms ≈ 1 connection, so 5 in all; 10 is twice that. 4 processes × 10 = 40, far under MySQL's default `max_connections` of 151. On this machine's loaded disk the pool did queue (thousands of waits), but 24 connections raised the claim p99 from 0.4–0.6 s to 1.2–1.3 s and lowered throughput in both rounds: the disk was the limit, not the pool. |
| `APP_PROVIDER_TIMEOUT` | 5 s → 5 s | 10 × the assumed p99, so a slow success is rarely cut (a cut is an unknown outcome and a retry). A hung provider then holds at most the concurrency's places, not 95/s × 5 s = 475 per channel. |
| `APP_LEASE_DURATION` | 30 s → 30 s | An eighth of it is the send alignment's ceiling (`leasebudget.AlignCeiling`, session adaptive-align): 3.75 s, above the claims of a loaded database (about 3 s at p95, owner item 0011). Floor (enforced): provider timeout + longest slot wait (ceiling + max(horizon, ceiling), a quarter of the lease once lease/8 ≥ 200 ms and the horizon ≤ lease/8) + one slot interval (10.5 ms at 95/s, the wait for a free call place) + lease margin 0.5 s. At the default 5 s timeout that solves to lease ≥ 7.35 s. At 30 s, a call that runs its full timeout after the longest wait still leaves 30 − 7.5 − 5 − 0.5 = 17 s, far above the slowest result write measured under overload (3.3 s). |
| `APP_REAP_INTERVAL` | 5 s → 5 s | Worst-case recovery after a crash = lease + reap = 35 s. Shorter adds little next to the 30 s lease, for one indexed `UPDATE` (on `lease_idx`) per worker per 5 s. The 10 s recovery this lane measured (lease 9 s, reap 1 s, below) needs a lease the alignment's ceiling cannot share: 9 s gives a 1.1 s ceiling, under the loaded-database claims. |
| `leasebudget.Margin` (was `leaseMargin` in `internal/delivery`) | 250 ms → **500 ms** | The fenced result write alone (one connection, no pool wait) measured p99 75, 138 and 452 ms in three runs on this loaded machine; plus NTP's tens of ms of skew (`lease_until` is set on the database's clock and read on the worker's), rounded up. 250 ms was below the measured p99. |
| `storeTimeout` | 10 s → **5 s** | At least the slowest result write measured under overload (3.3 s); at most the 5 s compose's 25 s stop grace sets aside for final writes after the 10 s shutdown grace, beside the 5 s telemetry flush and 5 s to spare. A write that misses it is retried by the lease (reaped, sent again under the same idempotency key). |
| `minSendTime` | 100 ms | A fifth of the assumed provider p99; a call given less is more likely cut than answered. Reached only after a pause. |
| `APP_SHUTDOWN_TIMEOUT` | 10 s | At least `APP_REQUEST_TIMEOUT` and `APP_PROVIDER_TIMEOUT` (enforced). Compose's `stop_grace_period` (15 s → **25 s**) holds it + 5 s for final result writes (`storeTimeout`) + the 5 s telemetry flush + 5 s to spare; at 15 s the flush alone filled the rest, and 20 s left nothing for a slow last write. A request still reading its headers at SIGTERM is not covered (`APP_READ_HEADER_TIMEOUT` runs before the request timeout). |
| `APP_REQUEST_TIMEOUT` | 15 s → **10 s** | The slowest legitimate request: an 8 MiB batch uploaded at 10 Mbit/s (6.7 s) + its insert + the 1 s publish budget, rounded up. That stays under the 60 s of common proxies, so the client gets this service's 503. Lowered so a request in flight at SIGTERM fits the 10 s grace. |
| `APP_READ_HEADER_TIMEOUT` | 5 s | Headers arrive in one round trip (under 1 s on a slow mobile link). 5 s bounds a slow-header client and stays far under nginx's 60 s `client_header_timeout`. |
| `APP_WS_SEND_BUFFER` | 256 → **1024** | One batch create publishes up to 1000 events at once, and an unfiltered stream gets 3 events per notification × 285/s = 855/s at the full rate; with 256, one batch landing while the socket is mid-write can drop a subscriber that is keeping up. 1024 holds one batch burst, at about 140 B per event 140 KB per socket. Derived, not measured. |
| `APP_IDEMPOTENCY_RETENTION` | 24 h | Stripe's 24 h key window, the common industry default. |
| `APP_IDEMPOTENCY_SWEEP_INTERVAL` | 1 min | Expired keys linger at most 1 min (0.07% of a 24 h table). |
| Telemetry flush (`cmd/app`) | 5 s | Free choice inside the 25 s stop grace (10 s shutdown + 5 s final writes + 5 s flush + 5 s spare); an export to a local collector takes milliseconds (OpenTelemetry's own export deadline, `OTEL_BSP_EXPORT_TIMEOUT`, is 30 s). |
| `app healthcheck` | 3 s | Between `/readyz`'s own 2 s and compose's 4 s healthcheck timeout, so each layer hears the inner layer's verdict. |
| `IdleMin`, `IdleMax` (`internal/delivery`) | 50 ms, 250 ms | At full load a call place frees every 500 ms / 72 ≈ 7 ms, so 50 ms wakes once several have freed and gives up at most about five of a channel's 10.5 ms slots to the other workers. 250 ms keeps a failing limiter or database at 4 retries a second per channel per worker. Free choices within those bounds. |
| `batchSweepLimit` (`internal/store`) | 1000 | Like `rollupSweepLimit`: a free choice that keeps one `DELETE` of small rows short; tune by its duration against the 100 ms retention-step target. |

Dials owned by other lanes keep their values and derivations, all in their doc lines and code comments: `APP_RATE_LIMIT_HORIZON` 200 ms (a budget of 10 limiter calls a second per channel per worker), the alignment's floor 200 ms, ceiling lease/8 and `claimSamples` 20 (adaptive-align); `APP_BREAKER_FAILURES` 5 (Envoy's consecutive_5xx), `APP_BREAKER_COOLDOWN` 5 s (Hystrix's sleep window) and `APP_MAX_ATTEMPTS` 10 (decision 0026: ceil(log2(cap/base)) + 1) (provider-health); `SafetyPoll` 1 s and the wake publish budget 100 ms (wake-hint); `RetentionBatch` 100 (measured DELETE times) and the rollup fold and sweep bounds (metrics-rollup); the realtime publish budget 1 s, `holdForPredecessor` 2 s and the reconnect back-off 100 ms to 5 s; the WebSocket ping 20 s. This lane's derivations agree with every one of them.

### Refused combinations (`config/cross_checks.go`, reported by `config.Load` and `app env check`)
| Rule | Why |
|---|---|
| `APP_LEASE_DURATION` ≥ `APP_PROVIDER_TIMEOUT` + longest slot wait (`leasebudget.SlotWait`: the alignment's ceiling + max(`APP_RATE_LIMIT_HORIZON`, ceiling)) + one slot interval (`limiter.SlotInterval`) + `leasebudget.Margin` | A claimed notification waits up to the ceiling plus the horizon for its slot and one interval more for a free call place (1 s at 1 per second), and its call must end the margin before the lease does. A shorter lease cuts calls before the provider timeout; each cut is an unknown outcome and a possible duplicate. The ceiling is the function the worker uses, so the rule follows the alignment. |
| `APP_SHUTDOWN_TIMEOUT` ≥ `APP_PROVIDER_TIMEOUT` | A call in flight at SIGTERM may last the whole timeout; the worker aborts it when the grace ends. |
| `APP_SHUTDOWN_TIMEOUT` ≥ `APP_REQUEST_TIMEOUT` | A request in flight at SIGTERM may last the whole request timeout; the server closes it unanswered when the grace ends. |
| `APP_RETRY_CAP` ≥ `APP_RETRY_BASE`; `APP_NOTIFICATION_RETENTION` ≥ `APP_IDEMPOTENCY_RETENTION` | (existing, moved into the same file) |

### Measured before and after
Command (one line): `go test -tags "integration measure" ./internal/delivery -run TestMeasureDials -v -cpu 2 -timeout 60m` (`dial_measurements_test.go`). It runs in-process workers against real MySQL 8.4, Valkey and the provider double, with the before and after sets back to back, alternating which goes first. The test process was pinned to two cores (GOMAXPROCS 2 and CPU affinity). About ten other sessions' containers ran on the machine at the same time. The database's write time alone moved between p50 16 ms and 58 ms from one run to the next (single-digit ms on an unloaded disk), so throughput carries about ±35% of noise between rounds. Each cell lists one value per round. Measured before adaptive alignment landed, with an "after" set of lease 9 s and reap 1 s that does not ship (see `APP_REAP_INTERVAL`): the recovery row shows what those two dials buy, not what ships.

| Scenario | Before (32, 30 s, 5 s, pool 10) | After (72, 9 s, 1 s, pool 10) |
|---|---|---|
| Recovery after a crash: claim to provider, 30 notifications | p50 30.9 / 31.4 / 30.9 s, max 31.7 s | p50 10.0 / 10.0 s, max 10.5 s |
| Provider at 500 ms, 2 of 3 workers, MySQL flush relaxed so the database is not the limit | 77.3 / 81.4 sends/s | 163.1 / 129.3 sends/s |
| Same, MySQL's normal flush (after set with pool 24) | 44.9 / 27.5 sends/s | 46.7 / 50.3 sends/s |
| Drain, 3 workers, instant provider, per-channel average | 6.7 / 13.3 (normal flush); 58.3 / 44.6 (relaxed) | 8.4 / 19.6; 40.7 / 40.8. No effect beyond the noise (after/before 0.70 to 1.47) |
| Pool 24 instead of 10 (drain, normal flush) | 37.7 / 57.7 per channel, claim p99 0.59 / 0.42 s | 16.0 / 22.6 per channel, claim p99 1.15 / 1.31 s |

The slow-provider gain is the concurrency's (Little's law caps 2 workers × 32 places at 128/s at 500 ms); the drain shows no effect beyond the noise.

### Next step at scale
At 24.6 M rows a day, deleting finished rows (retention, decision 0025) costs ~285 row deletes/s of steady write load on the same primary, in steps of 100 rows, and the B-tree keeps the churn. The next step: partition `notifications` and `delivery_attempts` by day (`PARTITION BY RANGE (TO_DAYS(created_at))`, the next partition created ahead each night). Retention then becomes `ALTER TABLE ... DROP PARTITION`: one metadata operation a day instead of 24.6 M row deletes, with no purge lag. MySQL requires every unique key to include the partitioning column, so the primary key becomes `(id, created_at)`, and lookups by id alone must still be served. The UUIDv7 id is time-ordered, so `created_at` can be derived from it and passed with every lookup. Written up, not built: it changes the schema's keys, every query by id and the claim index, and nothing in this repository runs at the volume that needs it.

Known limits: no acceptance test runs the new defaults (the harness pins its own); `dial_measurements_test.go` is the only run of them. The two acceptance fixtures that fell under the enforced lease floor were raised above it with the held acceptance tests (commit f6497d1).
