# Verifier report: adaptive send alignment (`git diff 873df76..9e80c94`)

Fresh-context `verifier`, 16 Nov 2026, on a machine saturated by other sessions. Saved verbatim below; how each finding was handled is under "Handling" at the end.

**Verdict: FAIL.** The rate cap holds: no way found for the change to push more than the grid's ~96 sends per sliding second past the limiter, and the jitter run's busiest second was 96. It fails for two reasons. The claim-time measurement that sets the lead also counts the deliberate deferral sleep (F1). And the disturbed-worker gap test failed on time, so its cap check never ran. `test/acceptance/` is identical to the base (empty diff).

(The verifier also noted uncommitted `DBGSEND` trace lines in the worktree while it ran: the coordinator's own temporary instrumentation for the cap investigation below; the code review is of HEAD.)

| # | Finding | Sev | Verdict | Evidence | Suggested fix |
|---|---|---|---|---|---|
| F1 | **Lead must measure the claim, but it also counts the deferral sleep** (item 3: "stuck high") | Medium | broken | `delivery.go:657-659` sleeps until `slots[0]-lead` when `deferClaim`; `:677-679` then records `time.Since(gr.asked)`, which includes that sleep. Timeline, all leads 20 ms, I=10.527 ms: a hint wakes A and B. A takes a free grid at V=0 (19 slots, 20–209.5 ms, TAT 220.0) and is flex. B takes at V=2 ms, before A's give-back, and gets one slot at 218 ms (deferred). B sleeps to 198 ms, claims (16 ms) and records ~216 ms. Below 20 samples the largest rules, and later any 2 in 20 do, so B's lead goes to ~216 ms. Once leads reach 200 ms or more, horizon = lead, a deferred sample is about lead + claim, and the lead creeps upward toward the 7.5 s ceiling on quiet hinted channels. The creep beyond ~220 ms is unproven. Cost: a burst on a free grid waits up to the lead. Every test fixes AlignFloor = AlignCeiling, so none covers adaptive lead with hints. | Start the sample after the deferral sleep, or skip observing deferred claims. Add a test with adaptive lead and 3 workers woken by hints. |
| F2 | **Disturbed-worker gap test did not finish within 45 s, so its cap check never ran** (R8) | Medium | unproven (load?) | `TestGapDeliveryR8RateUnderDisturbedWorker` stopped at sent 1063 / pending 392 / processing 45. Worker logs show "lease lost before the result was written; result discarded" and "release failed … lease lost" with the 2 s test lease. The ceiling is lease/4, so a row may wait lead + horizon = 1 s, half the lease (before: 0.4 s). There were also MySQL 1213 deadlocks, and MySQL took 43 s to start. Base not run for comparison. | Re-run on a quiet machine. If it repeats, cap lead + horizon at a smaller share of short leases. |
| F3 | **Lead stays high for over a minute after a slow spell ends** (item 3) | Low | holds as designed; cost understated | The lead drops only after 19 fast claims, and claims are at least `refill(horizon)` apart (`send_alignment.go:80`, `delivery.go:459,550`). At the 7.5 s ceiling that is about 19 × 3.75 s ≈ 71 s, with backlog rows waiting 11–15 s. The doc says "about 2 s … plus the slower refills". | Correct the doc, or count fast claims faster (e.g. refill from the pre-spell horizon). |
| F4 | **Test of flex sends now lets any send leave a lead early** | Low | test weakened | `clock_test.go:330` accepts any send down to `slot - AlignFloor`. Only flex groups may leave early; a non-flex group 150 ms early would pass. The i/i-2 spacing check still guards the cap. | Apply the early allowance to the flex group only. |
| F5 | **Limiter test fails when the machine pauses between two calls** | Low | test flaky (unchanged from base) | `limiter_integration_test.go:171` wants `First >= 50ms` from the second Take's now. With 12.1 ms between Takes, First was 47.858 ms for a slot correctly 60 ms after the first Take. | Compare `At+First` with the first grant's end. |
| F6 | **Give-back test only checks that GiveBack was called** | Low | missing test | `wake_test.go` checks `givenBack != 0`, a call count; the CAS result isn't asserted. No test runs give-back after a flex move, missed slots or a breaker cut while checking send spacing against the real Lua script. | Add an integration case for these. |

Attacks:
1. **Spacing under changing leads: holds.** TAT only moves forward in Take. A free grid needs TAT ≤ now, so earlier sends have left before now < now+lead. The limiter test with mixed leads (20 ms–1.6 s) showed a busiest window of 95.
2. **Lease: holds with the defaults.** Waits are at most 7.5 + 7.5 s, leaving 15 s for a 5 s call. clockMap span (1 s + ceiling + max(Horizon, ceiling)) covers it. Short leases see the pressure in F2. Lease 1 s with Horizon 2 s already overran before this change.
3. **Oscillation:** none; the feedback through batch size is monotone (gain b/I). It can get stuck high: F1, F3.
4. **One outlier: holds** once 20 samples are held. Before 20, a single slow outlier rules for 19 claims, as designed.
5. **Flex and give-back: holds.** Every send on a grant leaves before `from` = s_i − moved, so later slots start at `from` or later. A flex group needs Idle ≥ I, and its sends are not before N ≥ T+I. The CAS can only falsely succeed (ABA) when the later grant gave back everything and sent nothing, which is harmless. A clock step either voids the group, or is a forward step under one interval, which adds at most one request to a second, as before.
6. **Cap:** jitter run busiest second 96 on all three channels. The disturbed-worker run never measured it (F2).

| Command | Result | Duration |
|---|---|---|
| `go test -tags integration ./internal/delivery/limiter -count=1 -v` | FAIL: only TestLimiterReservesWithinTheHorizon (F5); others pass, busiest 95/95 | 38.9 s |
| `go test -tags integration ./test/acceptance/ -run TestGapDeliveryR8RateUnderLimiterReplyJitter -count=1 -v` | PASS, busiest 96/96/96, 2100 sent in 18.2 s | 55.6 s (pkg 71.0 s) |
| `go test -tags integration ./test/acceptance/ -run TestGapDeliveryR8RateUnderDisturbedWorker -count=1 -v` | FAIL: not all sent within 45 s; cap not measured | 105.6 s (pkg 122.0 s) |
| `go test -race -count=1 ./internal/delivery/... ./internal/notification/...` | PASS | delivery 34.1 s |
| `go vet -tags integration ./internal/delivery/... ./internal/notification/... ./cmd/... ./config/...` | clean | <10 s |

Not run: `slow_claim_integration_test.go`, the k6 thresholds and a fault test.

## Handling

- **F1 fixed.** A deferred claim starts its claim time after its wait (`claimGroup`). New test `TestDeferredClaimsDoNotLiftTheLead` (three workers woken together by hints, lead free to follow claims): fails without the fix (leads stuck at 26 ms against a 20 ms floor), passes with it.
- **F2 handled by lowering the ceiling.** `alignCeiling` is now lease/8, so a row waits at most a quarter of its lease for its slot (7.5 s of 30 s; 0.5 s of the test's 2 s), not half. The default ceiling, 3.75 s, stays above owner item 0011's claims (1.6 s; p95 of 0 to 3.2 s is about 3 s). Re-measured in the PR notes.
- **F3 fixed in the code, not only the doc.** Takes stay `refill(APP_RATE_LIMIT_HORIZON)` = 100 ms apart however far the horizon grew; a slow claim spaces them by itself (the next Take waits for it). Twenty fast claims after a slow spell so take about 2 s again, as the doc says.
- **F4 fixed.** `TestAGroupSentEarlyAfterIdleKeepsSendsOneIntervalApart` allows leaving before the slot only for the flex group's three sends (the first grant's slots); every later send is matched to a distinct slot of the later grants at or before it.
- **F5 fixed.** `TestLimiterReservesWithinTheHorizon` compares the second grant's first slot with the first grant's end, not with the second Take's now.
- **F6 partly.** `TestARowRightAfterALoneSendIsNotHeldBehindUnusedSlots` now asserts a GiveBack took effect, not only that it was called. No new integration case runs give-back after a flex move, missed slots or a breaker cut against the real script; the slowed-claim integration test exercises give-back against the real script under load, and its per-request trace (slot, leaving moment, arrival) showed no grid denser than 96 per second in three runs.
