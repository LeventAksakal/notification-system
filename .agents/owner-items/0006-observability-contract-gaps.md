# 0006 Observability/realtime contract gaps (raised by the observe test-writer)

## 1. Trace across the queue: continue or link?
Question: decision 0019 (recommended option 1) says the worker starts a NEW trace with a span link to the accepting request; the coordinator's test brief says the worker continues the stored trace, so the provider's `traceparent` carries the request's trace id. These conflict.
Options: (a) continue the trace (provider sees the same trace id, new span id); (b) new trace + link (provider sees a different trace id; the link is only visible in the trace backend).
Assumption taken: (a), as the coordinator's brief asks. `TestB5TraceparentToProvider` asserts the same trace id. If (b) is chosen, that assertion must change to "valid and different span id" only.

## 2. Readiness check values
Question: `Health.checks` is `map[string]string`; the spec does not say what value means healthy.
Assumption taken: keys `mysql` and `valkey`; value exactly `"up"` when healthy, any other non-empty value when not.

## 3. "windowSeconds echoed"
Assumption taken: a refused `windowSeconds` (0, 3601) is `400 invalid_request` with `details[].field = "windowSeconds"`; an accepted one is echoed in `Metrics.windowSeconds` (default 60).

## 4. Which correlation id goes on a later request's lines about a notification
Seen in the run: the `notification cancelled` line carries the cancel request's generated `correlation_id`, not the notification's stored one, alongside `notification_id`. The digest says "on every log line about that notification", which reads as the stored id. Options: (a) the stored id on lines about the notification (the request's id is still on the access line); (b) the request's id, as now. Assumption taken: none tested yet. The R14 test covers create and delivery only, so a cancel line does not affect it. If (a) is chosen, add cancel to R14.

## 5. WebSocket subscription liveness
Question: the spec does not say when a subscription is live.
Assumption taken: once the handshake completes, every later committed change is delivered (tests create right after `Dial` returns). The creation of a notification publishes a `pending` event; cancel publishes `cancelled`. Events with `batchId` subscription carry `batchId`.

## Answer (owner, in chat, 15 Nov 2026)
1. (a) Continue the trace: decision 0019 is now option 2.
2. and 3. The assumptions taken stand.
4. (a) The stored id goes on lines about the notification; the request's id stays on its access line. Built in session trace-correlation, with cancel added to R14.
5. Covered by decision 0013 and session realtime-seq (exact event order).
