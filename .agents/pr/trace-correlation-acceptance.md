# trace-correlation lane: acceptance tests for the test-writer

Branch `claude/feat-correlation-sampling-0f4bfd`. Nothing under `test/acceptance/` was written by this lane.

## Already present, no change needed
`TestR14CorrelationIDOnLogsAndProvider` (`test/acceptance/observe_logs_test.go`) already has the cancel case the brief asked for: "cancel with another correlation id: lines about the notification keep the stored id". It creates a notification scheduled 3 hours ahead with `X-Correlation-ID: A`, cancels it with `X-Correlation-ID: B`, requires at least one new serve line with `notification_id`, and asserts every such line has `correlation_id = A`. It passes against the code as built (the stored-id logging predates this lane).

## Optional additions (one assertion each, same test)
1. In the cancel case, assert the new cancel line has `request_correlation_id = B` (the name decision 0019's As built note fixes), and that the cancel's access line (`msg = "request"`, `path` ending in `/cancel`) has `correlation_id = B`. Proves a request's id still finds the request.
2. A replay case: create with `Idempotency-Key: K` and `X-Correlation-ID: A`, repeat the same body and key with `X-Correlation-ID: B`; the `notification create replayed` line has `correlation_id = A` and `request_correlation_id = B`.

Sampling is not an acceptance test: the compose stack runs the local default (100%), and the parent-based behaviour is proven in `internal/delivery` (`TestTheWorkerHonoursTheStoredSamplingDecision`).
