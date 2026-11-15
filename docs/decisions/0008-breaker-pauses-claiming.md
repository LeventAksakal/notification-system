---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when more than a handful of worker replicas run
---
# A per-channel circuit breaker pauses claiming, so an outage burns no attempts (0008)

## Question
When a channel's provider goes down, how do we stop every queued notification from being claimed, sent into the outage and burning its attempts, and stop the backlog from hammering the provider when it comes back?

## Rule
While a channel's breaker is open, workers do not claim rows for that channel. Only a real provider response or timeout counts as an attempt.

## Context
Bonus item "Failure Handling". With retries alone (0007), a provider outage of ten minutes pushes every in-flight notification through all its attempts and into `failed`, although nothing was wrong with any of them.

## Options
1. No breaker; backoff only. Nothing to build; an outage longer than the retry budget dead-letters healthy messages, and workers keep hammering a provider that is down.
2. A breaker per channel inside each worker process. Opens on a failure ratio over a short window, lets one probe through after a cool-down (half-open), closes on success. Each replica learns of the outage separately, which costs a few failed sends per replica.
3. Breaker state shared in Valkey. All workers stop and resume together and exactly one probes; more Lua and one more thing that fails when Valkey does.

## Decision
Option 2, with option 3 named in the README as the next step and proposed as a follow-up below. The unit is the channel because each channel has its own provider URL in configuration (all three default to the same provider double), so one channel's outage does not stop the others.

## Confirmation
Fault test: the provider double returns 503 for one channel for a fixed period. Assert: sends to that channel stop within the window, the other channels keep their rate, no notification on the failing channel reaches `failed`, and all are `sent` after recovery.

## Discussion with the owner (15 Nov 2026)
Owner: "on upstream outages we guard against thundering herds like you said correct? or if 1000 batch notifications came in each calls the DB and runs the worker only to get 5xx again?"

Answer, from the code (`internal/delivery/delivery.go:366`, `internal/delivery/breaker/breaker.go`); the counts are estimates from the default dials, the behaviour is what `TestGapDeliveryB1*` and the breaker tests assert:
- **Three guards, each for a different herd.** The rate limiter (0009) caps every herd, first sends and retries alike, at 95 per second per channel: the provider can never see more, outage or not. The breaker stops the herd from forming during the outage. Full jitter (0007) spreads the retries of messages that failed together.
- **1000 notifications arrive while the provider returns 5xx.** Each worker claims about 20 rows at a time, paced by the limiter. Its breaker opens after 10 calls in a 10 s window with at least half failed, so roughly 10 to 20 sends fail per worker, 30 to 60 across three. Those messages spend one attempt each and get a jittered due time. From then on the breaker is checked before every claim: while it is open the worker neither claims nor calls the provider for that channel, so the other roughly 950 rows stay `pending` with no attempt spent and no database work beyond the sleeping loop.
- **During the outage** each worker lets one probe through per 5 s cool-down: one claimed row, one call. Ten minutes cost about 360 attempts across three workers, spread over the backlog because a failed probe pushes its row's due time back. With 12 attempts per message (0007 as built) nothing exhausts.
- **On recovery** a probe succeeds and closes that worker's breaker; the others close on their next probe, within 5 s. The backlog drains at the paced rate: 1000 messages in about 10.5 s, never above 100 in a second.
- **Where the breaker does not help:** low traffic. Fewer than 10 calls in 10 s never trips it, so a lone message meets the outage with only its retry budget. That is why 0007's budget is now a stated span (about 12 minutes on average) rather than a count that ran out in 30 s.
- **Cost of per-process breakers:** each worker learns of the outage on its own (about 10 failed sends each) and probes on its own (three probes per cool-down instead of one). A breaker shared in Valkey (option 3) removes both, at the price of more Lua and one more thing that fails when Valkey does; it is worth it once many replicas run (this record's revisit trigger).

## Numbers (15 Nov 2026)
| Number | Value | Where it comes from |
|---|---|---|
| `APP_BREAKER_WINDOW` | 10 s | Unexplained |
| `APP_BREAKER_MIN_REQUESTS` | 10 | Unexplained. Consequence: under one call a second the breaker never trips, and a lone message meets an outage with only its retry budget |
| `APP_BREAKER_FAILURE_RATIO` | 0.5 | Industry default (Resilience4j's failure-rate threshold defaults to 50%) |
| `APP_BREAKER_COOLDOWN` | 5 s | Sets how fast sending resumes after recovery (at most 5 s) against probe cost (one call per 5 s per worker). Not derived from a target: unexplained |
| `probeWait` | 20 ms | Code constant in `internal/delivery/breaker/breaker.go`: how long a caller waits while the probe is out |

## Follow-up proposed (15 Nov 2026)
Owner: "I feel like we should be caching the health and readiness of upstream services we depend on in valkey or somewhere else no? isn't that done in the industry?" Proposed: keep each channel's provider health in Valkey, shared by every worker (option 3 above), gate claims on it, and count an attempt against a message only while its provider is believed healthy, so an outage of any length dead-letters nothing and the retry budget measures only failures particular to that message. The numbers above are then re-derived from targets (how fast an outage is detected, how fast sending resumes). Not built; it edits `internal/delivery`, so it starts after the wake-hint branch lands.
