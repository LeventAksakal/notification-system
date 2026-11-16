---
status: open
created: 2026-11-16
tags: [rate-limit, headroom, testing]
---
# The 100-per-second cap on a starved two-core host (0020)

## Question
With the late-write refusal in (decision 0009, as built 16 Nov), is pacing at 95 per second enough? Or should the headroom grow again so the cap holds even in-process on a starved machine?

## Context
- This machine idles at 30 to 40% CPU from unrelated work, and every run below is pinned to two cores.
- Acceptance `TestDeliveryR8RateLimit`, where workers, API and provider stand-in are separate processes. Busiest one-second window over 20 runs:

| Commit | Busiest window | Runs over 100 |
|---|---|---|
| `65292ae` (before 16 Nov's provider changes) | 96 to 102 | 2 of 20 |
| `b80b05e` | 96 to 106 | 3 of 20 |
| with the late-write refusal (`bf57c40`) | 96 to 100 | 0 of 20 |

- In-process `internal/delivery` `TestWorkersKeepTheRateWhileClaimsAreSlow`, where workers and the receiving `httptest` server share one pinned process:
  - 102 in one second in 1 of 3 runs before the fix (`4824e5e`);
  - 103 in 2 of 3 runs after it.
  - The receiver's timestamps are taken by goroutines starved in the same process, so those arrivals bunch whatever the sender does. These runs cannot separate that from a real late send.
- CI runners are clean, with 4 vCPU since the repository went public.

## Options
1. **Keep 95 per second.** Trade-off: the in-process slow-claim test can fail on a starved local machine; CI and the deployed shape (separate processes) are expected to hold.
2. **Pace at 92 per second (8% headroom).** Trade-off: 3% less throughput, and the floors and every "95" in the documents move with it.
3. **Measure the slow-claim test's cap at the sender, from leave times, instead of by receiver arrival.** Trade-off: it then no longer checks what a provider sees.

## Assumption taken
Option 1. CI on the merge is the check: if any CI run sees more than 100, take option 2.

## Answer
