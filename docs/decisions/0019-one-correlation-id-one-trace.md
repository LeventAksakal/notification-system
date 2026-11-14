---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when a second service joins the request path
---
# One correlation id and one trace follow a notification across the queue (0019)

## Rule
The correlation id and the W3C `traceparent` of the accepting request are stored on the notification row. Every log line about that notification carries the id, and the worker's span links to the stored trace.

## Context
The brief: "structured logging with correlation IDs", "health check endpoint", and as a bonus "distributed tracing". The request that creates a notification ends long before a worker sends it, possibly in another process and after a scheduled delay.

## Options
1. `X-Correlation-ID` is accepted or generated at the API, returned in the response, stored on the row, logged by API and worker through `log/slog` (JSON), and sent to the provider as a header. The worker starts a new trace per delivery with a span link to the accepting request's trace. A batch of 1000 does not become one trace with 1000 children that stays open for hours.
2. The worker continues the accepting request's trace as a child span. One waterfall per notification; a scheduled or retried send produces a trace lasting hours, and a batch produces one enormous trace.
3. Correlation id in logs only, no tracing across the queue. Enough for the core requirement; the bonus is not met.

## Decision
Pending. Recommended: option 1. Health: `/healthz` says the process is up; `/readyz` checks MySQL and Valkey and is what compose and a load balancer use.

## Confirmation
Acceptance test: send a request with a known correlation id; the provider double received that id, and the log lines of the API and the worker for that notification all carry it. A trace query shows the delivery span linked to the request span.

## As built (14 Nov 2026)
Built as option 2, not the recommended option 1: the worker's delivery span is a child of the stored `traceparent`, so one trace shows the request, the queue wait and the provider call (`TestB5TraceparentToProvider`). The cost named under option 2 stands: a scheduled or retried send makes a long trace and a batch a wide one. Owner items 0006 and 0007.
