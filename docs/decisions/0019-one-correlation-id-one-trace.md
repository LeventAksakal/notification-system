---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when a second service joins the request path
---
# One correlation id and one trace follow a notification across the queue (0019)

## Question
A notification is accepted by one request and sent later, possibly hours later, by another process: how do you follow one notification from the request to the provider in logs and traces?

## Rule
The correlation id and the W3C `traceparent` of the accepting request are stored on the notification row. Every log line about that notification carries the id, and the worker's span links to the stored trace.

## Context
The brief: "structured logging with correlation IDs", "health check endpoint", and as a bonus "distributed tracing". The request that creates a notification ends long before a worker sends it, possibly in another process and after a scheduled delay.

## Options
1. `X-Correlation-ID` is accepted or generated at the API, returned in the response, stored on the row, logged by API and worker through `log/slog` (JSON), and sent to the provider as a header. The worker starts a new trace per delivery with a span link to the accepting request's trace. A batch of 1000 does not become one trace with 1000 children that stays open for hours.
2. The worker continues the accepting request's trace as a child span. One waterfall per notification; a scheduled or retried send produces a trace lasting hours, and a batch produces one enormous trace.
3. Correlation id in logs only, no tracing across the queue. Enough for the core requirement; the bonus is not met.

## Decision
Option 2, chosen by the owner on 15 Nov 2026 after it was built (owner items 0006 and 0007): the worker's delivery span is a child of the stored `traceparent`. Only a child meets requirement B5 ("one trace links the API request, the queue wait and the provider call"), and the provider sees the request's trace id. The costs are accepted: a scheduled or retried send makes a trace hours long, which is correct though awkward in a waterfall; a batch of 1000 makes one trace of a few thousand spans, which trace backends handle. OpenTelemetry's messaging conventions allow either a parent or a link. Health: `/healthz` says the process is up; `/readyz` checks MySQL and Valkey and is what compose and a load balancer use.

## Confirmation
Acceptance test: send a request with a known correlation id; the provider double received that id, and the log lines of the API and the worker for that notification all carry it. A trace query shows the delivery span as a child of the request span (`TestB5TraceparentToProvider`).

## As built (14 Nov 2026)
Built as option 2, not the recommended option 1: the worker's delivery span is a child of the stored `traceparent`, so one trace shows the request, the queue wait and the provider call (`TestB5TraceparentToProvider`). The cost named under option 2 stands: a scheduled or retried send makes a long trace and a batch a wide one. Owner items 0006 and 0007.

## Owner review (15 Nov 2026)
- Option 2 kept, as above; the record now says so.
- Lines about a notification carry its stored correlation id; a cancel line used to carry the cancel request's own id (owner item 0006 §4). The request's own id stays on its access line.
- Sampling: 100% locally (every demo trace visible, small volume). Production uses the standard `OTEL_TRACES_SAMPLER=parentbased_traceidratio`, with a ratio derived from a trace-storage budget. It is parent-based, so the worker honours the API's decision.
- Both fixes built in session trace-correlation (branch `feat-correlation-sampling`), merged with the rest.

## As built (16 Nov 2026): whose correlation id a line carries, and how many traces are kept

### Correlation id on lines about a notification (owner item 0006 §4, option a)
- A line about a notification (`notification_id` set) carries in `correlation_id` the id stored on that notification, whichever request or worker writes it: `notification accepted`, `notification create replayed`, `notification cancelled`, `notification replayed` (retry of a failed one), and the worker's `delivery attempt`, release and status-change lines. `telemetry.WithNotification` and the worker's `jobContext` set it.
- The current request's own id keeps the name `correlation_id` on that request's lines that are not about one notification: the access line `request`, error and timeout lines. On a line about a notification written by a later request (replay, cancel, retry), the request's id is added as `request_correlation_id`. Filtering by a notification's id therefore finds its whole story, and filtering by a request's id finds that request's access line and the lines it wrote about notifications.
- A batch: `batch accepted` carries the batch's stored id, which is the creating request's id; each notification of a batch stores that same id, so its worker lines carry it too.
- Any new line about a notification (reschedule included) goes through `telemetry.WithNotification` with the stored id.
- Tests: `TestLinesAboutANotificationCarryItsStoredCorrelationIDWhicheverRequestWritesThem` (`internal/intake`, integration: create, replay, cancel, retry), `TestLinesAboutANotificationCarryItsStoredCorrelationID` (`internal/telemetry`), and the cancel case of `TestR14CorrelationIDOnLogsAndProvider`.

### Trace sampling
- The sampler is the OpenTelemetry standard pair `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG`, read by the Go SDK (v1.47.0) in `sdktrace.NewTracerProvider`; `internal/telemetry` passes no `WithSampler`, which would override them. They are declared in `config` like every setting (decision 0020: one schema in code), marked `PassThrough` because the SDK, not `Config`, reads them: documented in `app env`, and a value the SDK would silently replace with "sample everything" (an unknown sampler, a ratio outside [0, 1] or NaN) refuses to start, as does a ratio set beside a sampler that ignores it. A misspelt variable name is not caught (`app env check` knows only `APP_` names).
- Local default: unset, which is `parentbased_always_on`, 100%. Every demo trace is visible and the volume is a few thousand traces a day.
- Production: `OTEL_TRACES_SAMPLER=parentbased_traceidratio`, `OTEL_TRACES_SAMPLER_ARG=0.02`. Parent-based is required, not a preference: the API decides when the request arrives, the decision travels in the stored `traceparent`'s sampled flag, and the worker's delivery span is its child, so the worker keeps exactly the traces the API kept. A non-parent-based sampler in the worker would record delivery spans whose request span was dropped. `TestTheWorkerHonoursTheStoredSamplingDecision` (`internal/delivery`): an unsampled stored trace records no delivery span at ratio 1, a sampled one is recorded at ratio 0.
- What ratio sampling loses: 98% of failed deliveries have no trace. Their log lines (every one, with the correlation id) remain. Keeping every error trace needs tail sampling in a collector; not built. Revisit when a collector sits between the services and Tempo.

### Numbers
| Number | Value | Derivation |
|---|---|---|
| Sends per day at the ceiling | 24 624 000 | 3 channels × 100/s (`APP_RATE_LIMIT_PER_SECOND`) × 0.95 (5% `APP_RATE_LIMIT_HEADROOM_PERCENT`) = 285/s; × 86 400 s |
| Spans per sent notification | 2 | 1 API request span (otelhttp server; a batch request is one span for up to 1000 notifications, so at most 1 each) + 1 `deliver` span per provider attempt, 1 attempt when the provider is healthy. No database or provider-client spans are instrumented |
| Trace-storage budget (assumption) | 1 000 000 spans/day | About 1 GB/day at about 1 KB per stored span, about 30 GB a month. An assumption to replace with the real backend's quota; the ratio follows by the row below |
| Production `OTEL_TRACES_SAMPLER_ARG` | 0.02 | 1 000 000 spans/day ÷ (24 624 000 sends/day × 2 spans) = 0.0203, rounded down to 0.02: 492 480 traces, 984 960 spans a day at the ceiling, about 5.7 kept notifications a second |
| Delivery spans, upper bound | 492 480/day | Every attempt, retries included, takes a limiter slot, so provider calls never exceed 24 624 000 a day however many attempts each notification needs: × 0.02. Retries do not grow the trace volume |
| Room left for request spans | 25 376 000 traced requests/day | (1 000 000 − 492 480) spans ÷ 0.02, about 294 requests a second. Every HTTP request is a span (otelhttp wraps the whole mux): creates, reads, lists, `/docs`, health probes. Intake above the send rate and heavy read traffic are what exceed the budget; the ratio then follows by the same division |
