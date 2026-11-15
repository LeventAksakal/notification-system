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
