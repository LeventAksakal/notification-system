# Held acceptance tests: metrics from per-minute rollups (R13, decision 0014)

The guard blocks `test/acceptance/**` from lane worktrees (owner item 0002). These go to the test-writer after the lane lands, in `test/acceptance/metrics_test.go` beside the three R13 tests. They check what a client can observe through the API, against the compose stack and the provider double, as the existing R13 tests do.

## TestR13MetricsCountsWhatAClientJustDid
R13: "The endpoint's numbers match what a test just produced". The existing `TestR13MetricsDeliveryOutcomes` already checks this across two API instances.

Its addition: the numbers are exact at every second of the minute. The rollups cover whole minutes and the partial first minute comes from raw rows, so a test that only reads at one moment can miss the edge.
- Create 20 notifications on one channel with the double accepting all, and wait until all 20 read `sent`.
- Read `/v1/metrics?windowSeconds=N` for N in 1, 29, 30, 31, 59, 60, 61, 3600.
- Assert:
  - every window that covers all 20 sends reports `sent` 20 more than the reading taken before the batch, and `attempts` 20 more;
  - no window reports a negative count;
  - a window shorter than the time since the first send reports at most 20 more.
- Repeat the reads after crossing the next minute boundary (wait until the server clock's second wraps): the 3600 s window still reports exactly 20 more.

## TestR13MetricsLatencyWithinBucketError
R13 latency. The endpoint's percentiles are estimates from buckets at most 12.5% wide (0014 "As built").
- Schedule 10 notifications with `scheduledAt` 2 s ahead, and record each one's `createdAt` and `sentAt` from `GET /v1/notifications/{id}`.
- Compute the exact p50 of those latencies (from the later of scheduledAt and createdAt to sentAt).
- Assert the channel's `latency.p50` from `/v1/metrics?windowSeconds=60` is within 12.5% above the exact value and not below it by more than 1 ms, provided no other sends ran in the window. Run it on a channel no other test uses in parallel.

## TestR13MetricsUnknownAttemptsCountedApart
Owner item 0015: an attempt whose result never came is `unknownAttempts`, not `attempts`.
- Configure the double to hang for one notification beyond the provider timeout, so the worker's lease expires and the reaper gives the call up as unknown.
- Assert `unknownAttempts` grows by 1 and `attempts` and `failedAttempts` do not count that call.

This needs the double's hang mode and a lease short enough for a test. If the stack cannot be configured so, leave the test to `internal/store/metrics_rollup_integration_test.go`, which covers it ("reaped as unknown").
