---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when real provider error codes are known
---
# Retries are classified, backed off with full jitter, and stored as a due time (0007)

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
Pending. Recommended: option 1. Dial values are the owner's to set; proposed defaults: base 1 s, cap 5 min, 5 attempts, provider timeout 5 s.

## Confirmation
- A table test over status codes and error kinds pins the classifier.
- Fault test: 503, 503, then 202 ends `sent` with 3 attempts and growing gaps; a 400 ends `failed` after 1 attempt; a `Retry-After: 3` is not retried earlier than 3 s.
