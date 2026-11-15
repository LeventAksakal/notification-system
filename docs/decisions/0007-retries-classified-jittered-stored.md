---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when real provider error codes are known
---
# Retries are classified, backed off with full jitter, and stored as a due time (0007)

## Question
When a send fails, do we try again, how long do we wait, and where does the waiting happen?

## Rule
`provider.Classify` is the only place that decides retry or fail. A retry is written to the row as a new due time; no worker sleeps holding a notification.

## Context
The brief: "retry failed deliveries intelligently" and, as a bonus, "failure handling". Its "Delivery & Retry Logic" section leaves the design open.

## Options
1. Classify, back off, store.
   - Retry: network errors, timeouts, 408, 425, 429, 5xx. Permanent: every other 4xx. A 2xx whose body lacks `messageId` counts as sent and is logged as a contract warning.
   - Delay: `random(0, min(cap, base * 2^attempt))` (full jitter), never shorter than the provider's `Retry-After`.
   - The delay is stored as the row's due time, so a retry survives a crash and frees the worker and its rate-limit token.
   - After the attempt limit the notification is `failed` with its last error: the dead letter. `POST /v1/notifications/{id}/retry` puts it back.
2. Retry inside the worker with `time.Sleep`. Least code; retries are lost on restart and a sleeping worker blocks its slot.
3. Fixed interval retries. Predictable; after an outage every waiting message retries at the same instant.

## Decision
Option 1. Every failure gets the action its meaning calls for (owner, 15 Nov 2026): a refusal is final, an unknown or transient outcome is retried, the provider's `Retry-After` is obeyed. Defaults: base 1 s, cap 5 min, 12 attempts, provider timeout 5 s.

## Confirmation
- A table test over status codes and error kinds pins the classifier.
- Fault test: 503, 503, then 202 ends `sent` with 3 attempts and growing gaps; a 400 ends `failed` after 1 attempt; a `Retry-After: 3` is not retried earlier than 3 s.

## As built (15 Nov 2026)
The first defaults (5 attempts, base 1 s, cap 5 min) left the cap dead: the waits after attempts 1 to 4 are at most 2, 4, 8 and 16 s, so a notification gave up after 30 s of failures at worst and 15 s on average, and a provider outage longer than that dead-lettered every message that met it while traffic was too low to trip the breaker (0008). The budget is now stated as a span: `APP_MAX_ATTEMPTS` defaults to 12, so the waits reach the cap and a notification keeps retrying for about 12 minutes on average, 23.5 at most. `delivery.RetrySpan` (`internal/delivery/retry_budget.go`) computes both numbers; the worker logs them at start (`retry_span_mean`, `retry_span_worst`), and `TestTheDefaultRetryBudgetOutlastsATenMinuteOutage` fails if the default budget falls under ten minutes on average or the cap becomes unreachable. A smaller `APP_MAX_ATTEMPTS` stays allowed (a deliberate fail-fast choice); the log line shows what it costs.

## Numbers (15 Nov 2026)
The owner asked that every number say where it comes from. Unexplained ones are marked; the shared provider health change (see 0008's discussion) is to derive or remove them.

| Number | Value | Where it comes from |
|---|---|---|
| `APP_RETRY_BASE` | 1 s | First retry within 0 to 2 s, for failures that clear in seconds (a dropped connection, a restarting instance). Not derived from a measured target: unexplained |
| `APP_RETRY_CAP` | 5 min | The longest wait between two attempts, so also the longest a message can sit after the provider recovers. Not derived: unexplained |
| `APP_MAX_ATTEMPTS` | 12 | Derived: the smallest count whose waits reach the cap and average at least 10 minutes in total |
| 10-minute outage target | 10 min | The coordinator's choice on 15 Nov, not the owner's; it goes away if an outage stops spending attempts |
| `APP_PROVIDER_TIMEOUT` | 5 s | Trade-off between a hung call holding a concurrency slot and giving up on a slow provider that would have accepted. Not measured against the provider: unexplained |
| The 5 attempts first recorded | 5 | Recorded on 13 Nov only as a "proposed default" with no reason (owner item 0001); it made the cap unreachable |
