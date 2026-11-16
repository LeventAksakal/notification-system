# fix(delivery): a row right after a lone send is not held behind other workers' slots

> No push and no PR (the coordinator merges through `.agents/merge-queue.md`). This file is what the PR description would be. Base: `f6497d1` (integration, with the held acceptance tests).

The second row after a lone send (R7 tail, `TestGapDeliveryR7SecondRowAfterALoneSend`) took 385 ms on the three-worker stack against a 100 ms p99. The one-worker in-package test passed because nothing stacked on the flex group's slots. The acceptance test is right; the cause was in the worker and in the limiter's give-back.

## Cause (traced on the stack and in a three-worker in-package test)

1. **A stacked reservation blocked the give-back.** All three workers wake on the first row's hint. The first takes the flex group on the free grid (19 slots, 200-400 ms out), claims the row, sends it at once and gives the rest back. The give-back was a CAS on the TAT: it only worked if nobody had reserved after the group. A co-woken worker had already reserved after it and deferred its claim, so the CAS failed and the grid stayed booked out to 400 ms.
2. **Every worker sat out its pause.** After a Take a worker waits a refill (100 ms) before it reserves again. A hint during that pause was ignored, so the second row waited up to 100 ms even when the grid was free.
3. **A free grid shorter than an interval restarted a lead out.** Flex needed `Idle >= Interval`. A row due a few milliseconds after a lone send found the grid free for less than an interval, so it was not flex and waited for a restart at now+lead (200 ms).
4. **An empty flex claim kept the grid booked for a lead.** A flex grant whose claim found no row gave back from its first slot, now+lead, so the grid looked busy for the lead.
5. **A far grant claimed the second row.** A co-woken grant one interval past the lead (behind a near grant) deferred its claim, took the second row and sent it at its far slot (158 ms with a 150 ms lead, caught by the new three-worker test, not by the stack).

## Fix

- **Limiter: give-back in any order** (`limiter.go`, contract doc in `ports.go`). A give-back below a later reservation is remembered in `<key>:returned` (end → start). When the TAT reaches a remembered end, by a CAS give-back or a chain of them, the script follows the chain down. Safety: a range is recorded only while TAT ≥ its end, and the TAT can only come below that end by consuming the range, so no slot is handed out twice. The spacing proof is unchanged: each returned range starts where its owner's last request already left.
- **Woken grants further out than the lead are given back at once** (`channelLoop`), not deferred: a restart would be no later. Deferral stays for grants within the lead.
- **The pause ends on a revived priority** (`pauseWait`, `selector.revived`). A hint that makes an empty priority claimable again ends the pause. A channel with a backlog has no empty priority, so its Takes stay a refill apart.
- **Flex on any free grid** (`Idle > 0`). notBefore = max(N, T+I) keeps the first flex send more than an interval after the last earlier one. The proof is beside `group.flex`.
- **An empty flex grant gives back to the TAT before it** (the limiter's now less `Idle`), as if it had never been taken.

## Numbers

| Measurement | Before | After | Target |
|---|---|---|---|
| Second row after a lone send (`TestGapDeliveryR7SecondRowAfterALoneSend`), 30 pairs, two cores | p50 385 ms, p99 387 ms | **p50 12.3 ms, p99 18.9 ms** | p99 ≤ 100 ms: met |
| Three-worker in-package test (`TestASecondRowAfterALoneSendIsNotHeldBehindTheOtherWorkersSlots`), race, two cores | 158 ms in 1 of 5 pairs (threshold lead+refill) | ≤ 5 ms in 50 pairs (10 runs) | < lead/2 (75 ms): met |
| First send after idle (`TestDeliveryR7FirstSendAfterIdleIsPrompt`) | (passing) | p50 12.2 ms, p95 14.5 ms | p50 ≤ 60, p95 ≤ 200 ms: met |
| R8 backlog, 3 workers (`TestDeliveryR8RateLimit`): sms / email / push | (passing) | 94.5 / 95.0 / 94.5 per s, busiest second 96 | ≥ 88 per s, ≤ 100: met |
| Disturbed-worker gap test, busiest second | (passing) | 96 / 96 / 97 | ≤ 100: met |
| Limiter reply jitter gap test, busiest second | (passing) | 96 / 96 / 96 | ≤ 100: met |
| R9 high behind 5000 low (`TestDeliveryR9Priority`) | (passing) | 353 ms; low alone 95.0 per s | pass: met |

The cost of giving far grants back at once: only when more rows than the near groups hold arrive right after a lone send, the next Take finds those slots a pause sooner. A claim as slow as the lead then misses at most a pause of them once: refill/interval = 100 ms / 10.5 ms, about 9 slots at 95/s. The cap is unchanged: every slot is still one interval apart on the shared grid.
