# fix(delivery): a provider call of which no byte left is released, not recorded

> No push and no PR (the coordinator merges through `.agents/merge-queue.md`). Base: `1579bd6` (integration/decision-review).

Under a loaded full suite on two cores, two acceptance tests saw one more `delivery_attempts` row than provider requests for some notifications. TestGapDeliveryR12AttemptsAccountedExactly had 28 of 210; TestGapDeliveryB1BreakerBurnsNoAttempts had 4 of 200. Run alone they passed.

## Cause (from the coordinator's saved runs)

- B1 (`claude-gate-acceptance.log`, 360af12), notification `…78f0`: attempt 1 is `transient`, `http_status` 0, `timeout: provider timeout after 500ms`, 710 ms; attempt 2 accepted. The double counted 1 request.
- R12 (`claude-acc-timing.json`, 795261d): timeouts of 709-720 ms on the 500 ms timer, and a Valkey call that missed its deadline at the same moment.
- A call that ends 200-450 ms after its own timer means the worker process was descheduled. The transport's write goroutine had not run, so nothing was written; the double (which counts every request whose bytes reach it, even after the client gave up) never saw it. The worker still recorded a timeout.

**Not caused by 3c2d59e.** On 1579bd6, pinned to two cores here: R12 passed 14 times (10 alone, 3 beside a load generator, 1 in a full suite) and B1 passed 6 times. An A/B at f6497d1 cannot separate two commits that both pass alone, and timeouts on a starved process do not depend on the slot logic.

**The test's invariant is right for this path.** Decision 0005 counts a call that "may have been sent". A call of which no byte left cannot have been sent, and 0005 already gives such jobs back: "no call, no attempt".

## Fix

- `provider.Client` wraps every connection it dials (TLS: the connection underneath) and counts writes started on it. `httptrace.GotConn` marks the count when a call gets its connection.
- When the call fails, the client closes that connection, then checks the count. If no connection was obtained, or none was written to, it returns `provider.ErrNotSent` wrapping the cause. A write already started counts as sent. Under HTTP/2, another call's writes can only push the answer toward "may have left".
- `deliver` releases an `ErrNotSent` job: the `in_flight` row is deleted, nothing is recorded, the budget is unchanged and provider health learns nothing. New give-up reason: `call_never_left`.
- Decision 0005's list of release reasons names the case.

## Tests

- `TestSendTellsACallThatNeverLeftFromOneThatMayHave`: received but unanswered → may have left; no connection, or a refused dial → not sent.
- `TestConnUseCountsWritesStartedSinceTheCallGotItsConnection`.
- `TestSendNeverReportsNotSentForARequestTheServerReceived`: 400 concurrent calls with 0.5-20 ms timeouts. No call reported not sent reached the server; with 8 procs, 191 calls failed after their write and were counted as may-have-left.
- `TestWorkerRetryOutcomes`, new case: two never-left calls, then 202 → `sent` with one attempt.

## For the test-writer (no change made under test/acceptance)

R12 also checks that 202 responses equal accepted rows and 503 responses equal transient rows. A call that timed out *after* its write reaches the double and is answered, while the worker records a timeout (`transient`, no HTTP status). That is legitimate under 0005, so under load those two checks can still differ, by the number of such timeouts. Suggested wording:

- `503 responses == transient rows with http_status 503`;
- `accepted rows + timeout rows answered 202 == 202 responses`, or simply `accepted rows <= 202 responses`.

The per-notification `rows == requests` invariant stays exact.

## Proposal: R7 second-row test from 67 s to about 20 s (test-writer change)

`TestGapDeliveryR7SecondRowAfterALoneSend` took 66.7 s on two cores here: 30 pairs × (2 s quiet + about 0.05 s), plus warm-up. What the quiet has to guarantee, per channel:

- every reservation of that channel's previous pair has passed: lead + horizon = 400 ms at the defaults;
- the channel's workers are idle again: the post-Take pause is one refill, 100 ms.

The other channels' state does not matter, because each channel has its own grid and loop.

Proposed change:

- Keep 30 pairs. Nearest-rank p99 over 30 is the maximum, so what it proves is unchanged.
- Rotate sms, email, push as now, but wait only until *that channel* has been quiet for 600 ms (400 + 100 + 100 ms margin) since its last pair. Track a `lastPair[ch]` time; no global 2 s.
- A round of three channels takes about 0.15 s, so each pair waits about 0.45 s. Estimate: 30 × 0.5 s ≈ 15 s plus about 5 s of stack and warm-up, about 20 s.

The bound to keep: if the lead floor or the horizon dial changes, the quiet must stay at least lead + horizon + one refill, with the numbers taken from the same constants the harness sets.
