---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when an operator asks for Prometheus scraping
---
# The metrics endpoint is a JSON summary read from shared state; OpenTelemetry carries the time series (0014)

## Question
With several API and worker processes, how does the metrics endpoint report queue depth, success and failure rates and latency correctly, whichever instance answers?

## Rule
`GET /v1/metrics` computes its numbers from MySQL, never from counters held in one process, so every API instance returns the same answer.

## Context
The brief: "real-time metrics endpoint (queue depth, success/failure rates, latency)" for "both internal teams and API consumers". There are several API and worker processes (0002); a counter in one process sees only that process.

## Options
1. A JSON endpoint in the API contract: queue depth per channel, priority and status; success and failure counts and rate over the last 1 and 5 minutes; delivery latency p50, p95 and p99 (accept to sent), all from `notifications` and `delivery_attempts`, cached for one second. The same measurements also go out as OpenTelemetry metrics to the collector and Grafana already in the stack.
2. Prometheus text at `/metrics` from in-process counters. The operations standard; each replica reports only itself, so the numbers mean something only after a scraper adds them up, and queue depth still needs the database.
3. OpenTelemetry export only, dashboards in Grafana. Nothing to build; there is no endpoint, which the brief asks for by name.

## Decision
Option 1. It answers the brief's three numbers correctly from any instance with one curl. Prometheus text can be added later through the OpenTelemetry Prometheus exporter without touching this endpoint.

## Confirmation
Acceptance test: create N notifications against a provider double that fails a known share; the endpoint's depth, success count and failure count equal what the test produced, read through any API instance.

## As built (14 Nov 2026)
Not cached: every call reads MySQL in one read-only snapshot, so the numbers always agree with what a client just did. The window is a `windowSeconds` parameter (default 60, at most 3600), not fixed 1 and 5 minute windows. Queue depth is per channel (pending, scheduled, processing, oldest due age), not per priority. Through OpenTelemetry the workers export delivery attempts by channel and outcome, provider call duration, and slots given up by channel and reason, beside the HTTP server metrics; queue depth is not exported. Read cost at size: owner item 0012.

## Follow-up (15 Nov 2026)
The owner accepted this record and asked for "bucket aggregates and pre-computed views": per-minute rollups by channel and outcome with latency histogram buckets, written with each attempt's final outcome, so an hour-wide call reads about 180 rows whatever the traffic (target under 50 ms over a million attempts, against about 30 s extrapolated today). Built in session metrics-rollup (branch `feat-metrics-rollups`); its "As built" section carries the bucket derivation. Numbers here: default window 60 s and maximum 3600 s are arbitrary (the maximum also set today's worst-case cost); p50/p95/p99 is the usual latency convention.
