---
status: open
created: 2026-11-16
tags: [api, metrics, scale, retention]
---
# The metrics totals count every kept notification on every call (0017)

## Question
`GET /v1/metrics` returns `totals`: how many notifications are in each status, over the whole table. That count walks `list_status_idx` from end to end on every call. With retention now keeping finished rows for `APP_NOTIFICATION_RETENTION` (168 h by default), how should totals be served?

## Context
Measured 16 Nov 2026 on a MySQL 8.4 container (Docker Desktop, 128 MB buffer pool, machine shared with other sessions), 1,000,000 notifications: `MetricsStatusCounts` took 287 to 893 ms (median 355 ms), a covering index scan of every row. The delivery half of the endpoint now reads per-minute rollups (decision 0014, "As built: per-minute rollups") and takes tens of ms for a one-hour window, so totals are now the most expensive part of the call. The count grows with the rows kept, not with the window: at the full 300 sends per second and seven days' retention, the table holds about 180 million rows, so the scan would take about a minute.

## Options
1. Leave as built. Exact and simple; the call's cost grows with retention × traffic, and one client polling it loads the database.
2. Keep status counts in a table maintained by triggers (as the delivery rollups are), one row per status and channel, sharded if hot. Exact and constant time to read; every intake, claim, finish and cancel then writes a counter row, and the pending and processing rows are written by every claim and intake of a channel, so they need sharding and a throughput test (the rollup measurement method in `metrics_measure_integration_test.go` applies).
3. Drop terminal statuses (sent, failed, cancelled) from `totals` or replace them with counts over the window (already served by `delivery`), keeping live counts only for pending, scheduled and processing, which are bounded by the backlog and read from `claim_idx`. Constant cost; a contract change (the spec's `StatusCounts`).

## Recommendation
Option 3 for the API (what an operator acts on is the backlog and the recent outcomes, both served), or option 2 if all-time counts per status must stay.

## Assumption taken
Option 1: totals unchanged on the metrics-rollups branch.

## Answer
