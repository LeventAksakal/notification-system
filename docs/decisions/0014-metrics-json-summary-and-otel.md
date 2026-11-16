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

## As built: per-minute rollups (15 Nov 2026)
The owner accepted this record on 15 Nov 2026 and asked for bucket aggregates and pre-computed views (owner item 0012 measured the hour-wide call at 0.9 to 1.8 s with 45,000 attempts, extrapolated to about 30 s at a million).

- Delivery (sent, failed, attempts, failed and unknown attempts, latency percentiles) is read from two rollup tables, `attempts_per_minute` and `finishes_per_minute` (`schema/delivery_rollups.sql`), one row per minute, channel and outcome (and latency bucket for sent). A rollup row equals a recount of the rows it describes: AFTER INSERT and UPDATE triggers on `notifications` and `delivery_attempts` take a row's old contribution out and put its new one in, in the transaction that changes it, whoever writes it (the result write, the reaper's and a release's `unknown`, a late result after a lost lease, a replay). A trigger rather than an upsert in the result write, because with attempts written in flight (0005) four code paths write outcomes and only a trigger sees each change's old and new value. While its minute is written, each key is spread over 4 shard rows, chosen from the counted row's id: a rollup row stays locked until the transaction that counted into it commits, and unsharded, every finish of a channel queued on one row (see Numbers). A change takes its old contribution out of the first shard of the key that holds one (only the sum means anything), and runs once more if that changed nothing, because a take-out that waited on a fold resumes past the shard 0 row the fold inserted. The retention pass folds a minute's shards into shard 0 two minutes after the minute starts (`store.FoldRollups`), so past minutes cost what unsharded rollups did to read.
- A window `[now - windowSeconds, now]` is read as the rollups of every whole minute from the first minute boundary at or after its start (the current minute included: its rows commit with the outcomes they count, so a client reads its own writes), plus the leading partial minute from the raw rows on the same timestamps (`started_at` for attempts, `finished_at` for notifications). Of that partial minute the read takes the shorter side of the window's start: the raw rows after it added to the next whole minute, or the raw rows before it subtracted from its own minute's rollups. So it reads at most half a minute of raw rows, whatever the window. All of it runs in one read-only REPEATABLE READ transaction, so the rollups and the raw rows are one snapshot.
- Percentiles come from log-linear latency buckets (`internal/store/latency_buckets.go`): every ms up to 16 ms, then 8 buckets per power of two, so a bucket is at most 1/8 of its lower bound wide. The nearest rank is located in its bucket and interpolated linearly inside it (Prometheus' `histogram_quantile`), so a percentile is within its bucket's width of the exact one: exact to 16 ms, under 12.5% above. Counts and rates stay exact.
- Queue depth and the status totals stay live index reads (`claim_idx`, `list_status_idx`).
- Rollup minutes are deleted by the retention sweep with the rows they count, at one minute bound: rows finished before, and rollup minutes starting before, the start of the minute that `now - APP_NOTIFICATION_RETENTION` falls in (so a row is kept up to a minute past the retention). A window, at most the retention's 1 h floor, then never starts in a minute whose rows or rollup are partly gone; deleting them at different bounds made the partial-minute subtraction return negative or inflated counts (verifier, 16 Nov 2026). The endpoint never reads past an hour, so a shorter rollup retention would do for it; the same retention is kept because a rollup minute then always has its rows beside it, so the recount test and any operator query can check it, and it costs little: about 161 rows a minute once folded (measured), 1.6 million rows over 168 h, under 1% of the 170 million notifications the same week holds at 95 sends per second per channel. Its sweep deletes at most 1,000 rows per statement on the primary key.
- Clock skew: attempts are keyed by `started_at`, the worker's clock, and deleted with their notification by `finished_at`, the database's. A worker clock running fast can stamp an attempt in the minute after its notification finished, so at a window of exactly the retention the subtraction side can count that attempt once too often. The error is bounded by the skew times the traffic at that minute boundary.
- Deploy: the migration starts the rollups empty (no backfill). For the first hour after it, a window that reaches back before the migration counts only what happened since, and on the subtraction side of its first minute can count below zero; a row written before the migration and changed after it (a late result, a replay) takes its old contribution out of the migration minute's count, if any. Read windows that start after the migration are exact.

### Numbers
Every dial of the rollups, why it has its value, and how to tune it. Measurements: 16 Nov 2026, 1,000,000 notifications over an hour, MySQL 8.4 in Docker Desktop (128 MB buffer pool, shared host); details in `.agents/pr/metrics-rollup.md`.

| Number | Value | Why | Tune |
|---|---|---|---|
| Latency sub-bucket bits (`latencySubBucketBits`) | 4: exact to 16 ms, then 8 buckets per power of two | A bucket (at most 12.5% wide) is narrower than the noise at each latency: 16-32 ms from 128 to 512 ms against the 50 ms poll step; 512 ms at the 5 s timeout against 100% retry jitter. Measured p95 +1.4%, p99 +0.13% | One bit more halves the error and doubles a busy minute's rows; check `latency_buckets_test.go` and the rows per minute |
| Raw part of the first minute (`halfMinute`) | at most 30 s | Reading the shorter side of the window's start halves the raw rows; the 1 h read is 21 ms at a whole minute, 45-46 ms at 30 s | A finer second rollup level would cut it further at the cost of a second write per outcome |
| Shards per rollup key | 4 | Unsharded, one row per channel and minute capped a channel at 128-130 finishes/s and the rate test fell to 58-84/s; with 4, 256-276/s and the rate test holds 95/s | More shards cut lock waits and multiply the rows of minutes being written |
| Fold of a minute's shards | 2 min after it starts, for minutes up to 10 min old | A minute's calls end within the 5 s provider timeout, so it is idle a minute after it ends; folding keeps an hour at 9,694 rows (14 ms) instead of 26,630 (34 ms); the span covers eight missed passes at the 1 m sweep interval | A longer sweep interval needs a longer span; an unfolded minute is exact, only slower |
| Rollup retention | `APP_NOTIFICATION_RETENTION` (168 h default, 1 h floor), at the rows' minute bound | A rollup minute always has its rows beside it to check against; 161 rows a minute folded is under 1% of the week's notifications | Could drop to the 1 h window if storage mattered |
| Rollup sweep batch (`rollupSweepLimit`) | 1,000 rows | About six folded minutes per statement, one minute's rows per pass at the 1 m interval, on the primary key | Raise if a sweeper falls behind |
| `claim_idx` gains `attempts`, `finished_idx` gains `created_at, scheduled_at` | — | The scheduled count read the table (6.6 s at 1M rows, 10 ms covered); the raw partial-minute finishes read the index alone | Drop with the reads that need them |
