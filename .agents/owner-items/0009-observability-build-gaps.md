# 0009 Observability build: contract gaps found while implementing (raised by the observe implementer)

## 1. Rates are generated as float32
Question: `DeliveryTotals.successRate`/`failureRate` and `DeliveryStats` are `type: number` without a format, so oapi-codegen emits `float32`. JSON then carries 7 significant digits (15/18 -> 0.8333333); `TestR13MetricsDeliveryOutcomes` compares to 1e-9.
Options: (a) add `format: double` to the four rate properties in `api/openapi.yaml` and regenerate; (b) keep float32 and loosen the test.
Assumption taken: (a) is the intent. Until then `internal/api/metrics.go` wraps the generated `Metrics`/`DeliveryTotals`/`DeliveryStats` and encodes the two rates as float64 (same JSON shape). After (a), delete the wrappers and return `GetMetrics200JSONResponse`.

## 2. Live events cannot show `scheduled`
Question: the digest says the API shows `scheduled` for a pending, never-tried notification due in the future. `core.Event` has no due time, so the WebSocket can apply that view to the snapshot (read from MySQL) but not to live events: creating a scheduled notification publishes `pending`.
Options: (a) add `DueAt time.Time` (or `Scheduled bool`) to `core.Event`, set by intake on create/replay; the stream then shows `scheduled`; (b) document that live events carry stored statuses and only the snapshot carries the view.
Assumption taken: (b) for now; no test covers it. (a) touches `internal/core` and `internal/intake`, neither owned by this lane.

## 3. Events have no commit sequence
Question: ordering between processes (API create vs worker claim) is restored in the hub from `(status, attempts)`, the publisher's role and `at` (see `internal/realtime/order.go`). A per-notification sequence (e.g. `claim_version` plus a status step, or `updated_at` from the row) carried in `core.Event` would make ordering and snapshot de-duplication exact instead of rule-based.
Assumption taken: rule-based ordering, as built. Residual: a release or reap racing a reclaim, judged by `at` across two worker clocks.
