# feat(delivery): send alignment follows measured claim time

> No push and no PR (the coordinator merges through `.agents/merge-queue.md`). This file is what the PR description would be. Base: `873df76` (wake-hint; code identical to `1b74a74` on `integration/decision-review`).

A worker used to reserve its send slots a fixed 200 ms ahead of its claim. While the database answered a claim in 1.6 s (owner item 0011), every slot passed before its row was in hand and the channel sent almost nothing. The alignment (the lead) now follows the worker's measured claim time, and the horizon grows with it. On the owner-item-0011 scenario the fast channels drain at **95.0 per second** (base: 1.9 and 5.2). The busiest second stayed at 96 or less in every rate test. On a healthy database nothing got slower.

## Results

All runs are on this machine with other sessions' stacks running (CPU load 21-74% while measuring). Base is `873df76` in a temporary worktree, run in the same hour.

| Measurement | Base (fixed 200 ms) | This branch | Target |
|---|---|---|---|
| Owner-item-0011 scenario, every claim 1.6 s (`TestWorkersKeepTheRateWhileClaimsAreSlow`): email / push, whole drain | 1.9 / 5.2 per s (80 and 240 of 900 sent in 60 s) | **95.0 / 95.0 per s**, busiest second 96 / 96 | ≥ 90 per s, ≤ 100: met |
| Same, claims spread over 0-3.2 s: email / push, whole drain | failed to drain in 60 s | 80.3 / 62.8 per s (push lost 3 s at the start), busiest 96 / 96 | not a target; reported |
| R8 backlog, 3 workers (`TestDeliveryR8RateLimit`): sms / email / push, whole drain | 89.2 / 90.7 / 91.9 per s, busiest 96 | **94.1 / 94.6 / 93.3 per s**, busiest 96 | ≥ 88 per s, ≤ 100: met |
| R9 high behind 5000 low (`TestDeliveryR9Priority`), POST to provider, 4 runs each | 341, 394, 403, 403 ms | 336, 352, 355, 414 ms | no regression: met |
| R9 low still moves; low alone ≥ 93 per s | pass | pass | pass |
| Disturbed-worker gap test, busiest second | 96 / 96 / 96 | 96 / 96 / 96 (pass in 24.5 s) | ≤ 100: met |
| Limiter reply jitter gap test, busiest second; average | 96 / 96 / 97; 65.0-71.3 per s | 96 / 96 / 96; 93.9-94.9 per s | ≤ 100: met |

R7 (a lone send after idle) is unchanged: on an idle grid a group leaves right after its claim whatever the lead (`group.flex`, wake-hint).

**Latency while claims are slow.** The lead is the p95 of the newest 20 claims, so about 1.6 s in the 0011 scenario, and the horizon is the same 1.6 s. A row claimed on a busy channel waits for its slot, at most lead + horizon (3.2 s; capped at a quarter of the lease, 7.5 s by default). The row's whole latency is that wait plus the claim. A lone row on an idle channel waits only for its claim. Once claims are fast again, the lead is back at the floor after 20 claims, about 2 s (Takes stay 100 ms apart).

## Design

- **The lead goes to the limiter, not onto the slots.** `Limiter.Take` gets `SlotRequest{N, Lead, Horizon}`. A free grid (TAT ≤ now) restarts at now+lead, and a busy grid continues at its TAT. Every slot handed out is the send moment itself, on the one grid all workers share. The TAT only moves forward, so slots stay one interval apart whatever lead each Take asked with. The spacing proof is the old one, unchanged: each request leaves in [slot, slot + interval). A lead added on the worker would not have this property: two workers with leads 1.5 s apart could each send a full second's slots into the same second. The proof is beside the code in `send_alignment.go`.
- **The measured claim time (`claim_latency.go`).** This is the time from just before the Take to rows in hand, over the newest 20 claims. A worker that has measured all 20 takes the second largest (their p95). One outlier costs only its own group, and a second slow claim lifts the lead at once. With fewer than 20, it takes the largest. A deferred claim (`group.deferClaim`) starts its clock after its wait.
- **Horizon = max(`APP_RATE_LIMIT_HORIZON`, lead).** A worker reserves once per claim. With a horizon shorter than the claim, a lone worker would send at horizon/claim of the rate: 200 ms per 1.6 s claim is an eighth. Takes stay `refill(APP_RATE_LIMIT_HORIZON)` = 100 ms apart however far the horizon grew.
- **Unused slots go back (`Limiter.GiveBack`).** This is the follow-up named in the wake-hint PR. A group that found fewer rows than slots returns the rest. A CAS on the TAT moves the grid's next free slot back to the first unused slot, but only if nobody reserved after it. Every request already sent on the grant left before that slot, so the spacing holds.
- **Borrowed slots skip priorities never answered for (`Worker.claim`, `selector.known`).** Before, a claim whose priority ran short lent the slots to the others in the same claim. A priority with no weight was asked that way too, even though nobody knew whether it had rows. Each such ask is one more claim query. At 1.6 s a query, one short claim took 4.8 s, every worker missed its whole group at once, and the channel stopped for 4 s. Those priorities are now looked up with `Queue.NextDue` after the sends are started (as safety polls already were), and the next round claims one that has a row due. With weights 100/0/0, a high-only test checks that no claim query goes to normal or low. It fails without the change (3 + 3 queries). A second test checks that rows of a priority with no weight are still sent.

### Derived numbers

| Number | Value | Why |
|---|---|---|
| Floor (`alignFloor`) | 200 ms | The fixed alignment this replaces, so the lead only ever grows. The floor has to cover the jumps in claim time, not the typical claim. As R8's drain ended, claims rose from 20-70 ms to 200-445 ms within one second (nine claimers on the last rows; debug trace). That is faster than any window of past claims can follow. With a 20 ms floor (the median healthy claim rounded up), R8 drained at 76-82 per second and failed its 88 floor. It costs only latency on a busy grid; R9 above shows none. |
| Ceiling (`alignCeiling`) | lease / 8 = 3.75 s at the default 30 s | A row then waits at most lead + horizon = a quarter of its lease, leaving three quarters for the provider call (5 s) and the result write. A quarter of the lease as the ceiling (half the lease waiting) lost leases with the test's 2 s lease (verifier F2). 3.75 s is above 0011's claims: 1.6 s, and the p95 of claims spread over 0-3.2 s is about 3 s. |
| Samples | 20, second largest (p95) | Counted in claims, not seconds. A time window holds about 100 healthy claims but only a few slow ones, and a worker that makes no claim would come back with nothing measured. 20 healthy claims take 2 s. |
| Start | 200 ms (= floor) | Before two claims are measured. |
| Take spacing | `refill(APP_RATE_LIMIT_HORIZON)` = 100 ms | A budget of 10 Takes per second per channel per worker, as before. |

## Verifier

A fresh-context verifier reviewed `873df76..9e80c94`. Its report and the handling of each finding are in `.agents/reviews/2026-11-15-adaptive-align.md`. In short:

- F1 (the deferral sleep counted as claim time): fixed, with a test.
- F2 (the disturbed-worker test ran out of time with a 2 s lease): ceiling lowered to lease/8; the test now passes in 24.5 s.
- F3 (the lead stays high a minute after a slow spell): Takes stay 100 ms apart.
- F4 (a flex test was weakened): fixed.
- F5 (a flaky limiter test): fixed.
- F6 (give-back was only checked as called): partly handled; the give-back test now asserts the slots were returned.

After the verifier, two changes were found by measurement: the floor back at 200 ms and the borrowed-slot rule.

## Gate

| Command | Result | Duration |
|---|---|---|
| `go tool -modfile=tools.mod task check` (first run) | FAIL: lint, 5 findings in `slow_claim_integration_test.go` (its trace knobs read `os.Getenv`; fixed in `test(delivery): slow-claim trace knobs pass the linter`) | 51 s |
| `go tool -modfile=tools.mod task check` (after the fix) | PASS: 503 tests, 0 lint issues, no vulnerabilities | 74 s |
| `go tool -modfile=tools.mod task test:integration` | FAIL: 177 tests, 2 failures, both throughput floors; the cap held (busiest 96). `TestDeliveryR8RateLimit` drained at 84.7-86.3 per s (floor 88). `TestDeliveryB1BreakerPausesChannel` sent email and push at 76.5 per s during the sms outage (floor 88). The gate runs the integration packages in parallel on a machine that was already loaded. | 496 s |
| B1 alone, twice | PASS: 94.9 and 92.2-93.2 per s | 12 s each |

**R8's floor is less reliable on this branch than on base under load.** All runs below used the final code, on this machine, the same evening. The cap held at 96 in every one.

- **This branch:** 6 of 11 runs met both floors, at 89-95 per s. The 5 that failed drained at 79-88 per s. One more drained at about 50 per s because of a 4 s stall at the tail on all three channels. The stall did not come back in 5 traced reruns and is unexplained.
- **Base:** 3 of 4 met both floors (89-95 per s). The fourth failed only the steady-state floor.

**What the traces show.** In the traced failures, claims jumped to 300-850 ms at the start or at the tail of the drain. The lead is capped at 250 ms by the test's 2 s lease (an eighth of it), and both branches lose those slots.

**Where this branch is weaker.** Base added its 200 ms on top of every slot, so a claim always had at least 200 ms before its first slot. Here a busy grid continues at its next free slot (TAT), so the margin is the TAT minus now. That margin is at least the lead in steady state, but it can be only milliseconds when every other worker is stuck in a slow claim and the reservations run thin. A worker's first slots are then missed, and the rows claimed for them are released, which wakes every worker. Starting every reservation at now plus the lead would remove that, but on a healthy database it wastes the slots that typical claims (20-70 ms against a 200 ms lead) use today, so it was not done.

**Proposed follow-up:** make a miss cheap instead of rarer. Keep the rows of a claim that outran its slots, and reserve new slots for them with no lead (the rows are in hand), instead of releasing them and claiming again.

## After the rebase onto `integration/decision-review` (c294d99, 16 Nov)

Integration replaced the per-worker breaker with the shared health gate (provider-health), so in `internal/delivery` integration's side was kept and the alignment re-applied on top. What needed adapting:
- **`probeLease`:** the probe's slot is now at most lead + horizon ahead, both at their largest, with no `SendAlign`.
- **The limiter:** `limiter.New` drops its horizon argument again (the horizon travels with each `SlotRequest`), so integration's two measurement tests changed with it.
- **The slowed-claim test** uses the health gate (5 failures, 1 s cool-down, a key prefix per case).
- **Race fix:** a race in my own test, `TestDeferredClaimsDoNotLiftTheLead`, read the grid under the wrong lock.

Scoped checks on the rebased tip, every test process pinned to two cores (CPU affinity 3):

| Check | Result | Duration |
|---|---|---|
| `task gen` | no changes | 8 s |
| `go build ./...`, `go vet ./...`, `go vet -tags integration ./internal/... ./cmd/... ./config/...` | clean | 15 s |
| `go test` delivery, notification, config, cmd | PASS | 38 s |
| `go test -race` delivery, notification | PASS (after the test fix) | 52 s |
| `go test -tags integration ./internal/delivery/...` | PASS | 111 s |
| Slowed scenario, every claim 1.6 s: email / push | 95.1 / 95.1 per s, busiest 96 | 13 s |
| Slowed scenario, claims 0-3.2 s: email / push | 94.0 / 93.9 per s, busiest 96 | 14 s |
| `task lint` | FAIL, none in this lane's files. 9 findings in `internal/intake/reschedule*`; `test/acceptance/delivery_wake_test.go` uses `n.ID` where the generated client has `Id`, so the package does not typecheck. Both are on the integration tip. | 29 s |

**Not run on the rebased tip:** the acceptance tests (R8, R9, the gap tests, B1). `test/acceptance` does not compile on the integration tip. The R8 numbers above are from before the rebase.

## Not verified

- **One reading of 108 in a second** on an earlier commit (`9e80c94`), never reproduced. In three traced runs, the per-request trace (slot, leaving moment, arrival) showed no grid denser than 96 per second. Up to 98 arrivals came from requests bunched in transit. The verifier found no path past the grid. It stays unexplained.
- **The 0-3.2 s case** lost 3 s at the start on one channel: the first claims (up to 3.2 s) outran the 200 ms start lead. Not a target. A start lead taken from the other channels' measurements would fix it.
- **Timing tests that fail under load** on base too: `TestWorkerShutdownReleasesUnsentWork` (fixed here with 25 ms slots), `TestABacklog…`, `TestWorkersAcrossSmallLimiterClockSteps`.
- **Machine load** was never below 12% for any of these measurements. Numbers on a quiet machine may be higher, the base included.

## Follow-ups

- **Keep the acceptance workers' logs** (test-writer, `test/acceptance/harness_test.go`). Today the harness prints only the last 60 lines, and only on failure. A temporary local patch found R8's cause in one run: an env var such as `ACC_LOG_DIR` that writes each child's full log and sets `APP_LOG_LEVEL=debug`.
- **The held acceptance tests** are in `.agents/pr/adaptive-align-acceptance.md`.
