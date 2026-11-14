# Whole system: fresh-context verifier report (14 Nov 2026, Opus, on `pr/b-delivery-observe`)

Verdict: **fail**, on finding 1. The verifier could not run vet, the race tests or the acceptance package: the guard accepts only commands that begin `go test` or `go vet`, so `go -C <snapshot>` was refused. Those were run by the coordinator (acceptance: 55 tests, 204 s, pass) and by CI on the same branch (gate green, 16 min 38 s).

| # | Severity | Finding | Where | Reproduced | Status |
|---|---|---|---|---|---|
| 1 | High | With Valkey down a 1000-item batch POST does not return for about 1000 s: one publish per item, each waiting up to 1 s. Rows are already committed, so a client that retries without a key creates duplicates | `internal/intake/service.go:218`, `internal/realtime/publisher.go:26,100` | yes | fixed `598e3c5`: one pipelined publish under one deadline; 1000-item batch with Valkey hung answers in 0.2 to 1.3 s (`TestGapOutageBatchAcceptedPromptlyWithValkeyDown`) |
| 2 | Medium | WebSocket subscribers get no signal when the hub loses and regains its Valkey subscription; changes in the gap are lost silently | `internal/realtime/hub.go:155-178`, `internal/api/ws.go:121` | inferred | fixed `346e888`: streams close with 1012, upgrades get 503 until the feed is back (`TestGapOutageWSClosedWhileFeedLost`) |
| 3 | Medium (process) | The rate test's floor change sat inside the implementer's `feat(api)` commit. Cause: the slice C lane rebuilt its commits with `git commit-tree` and `update-ref`, which swallowed two coordinator commits (the test change by a test-writer, and the compose change) | history of `pr/b-delivery-observe` | yes | fixed: history re-split into four commits, tree identical; pull request 4 replaced |
| 4 | Low-medium | The event sequencer scans its whole map every 25 ms under the hub mutex; no size cap | `internal/realtime/order.go:90-94` | inferred | fixed `346e888`: entries kept in time order, both maps capped at 50,000 (`TestSequencerStaysWithinItsCap`) |
| 5 | Low-medium | `/v1/metrics` is uncached and scans the status index | `internal/store/queries/metrics.sql` | inferred | kept: a cache would make the endpoint lag what a client just did; noted in the README as an operations concern |
| 6 | Low | The five metric queries run on separate snapshots; `totals.pending` can be off by rows created in between | `internal/store/metrics.go:111-163` | inferred | fixed `323dcd7` |
| 7 | Low | Latency was measured from `due_at`, which a retry or release moves; waiting before those is not counted | `queue.sql:37,50` | inferred | fixed `323dcd7`, spec 1.2.0 |
| 8 | Low | Under a slow Valkey a `processing` event can be released before its `pending` | `internal/realtime/order.go` | not reproduced: 2999 events of a 999-item batch, none out of order | covered by 1 (batch publish is one pipeline) |

Compose proofs (project `notifier-prb`, app on port 18080 because 8080 is taken on this machine): one command, all healthy in 1 to 2 minutes. 3 × 1000 backlog: busiest second 98 on every channel, drained in about 10.8 s, `sent` 3000. A worker replica removed mid-drain (graceful stop): all 3000 `sent`, none `processing` after the lease, 0 keys delivered more than once. Valkey stopped: `/readyz` 503 at once; started: 200 after 5.7 s and delivery resumed. Trace id sent with the request arrived at the provider.

README walk: the template example's `scheduledAt` in 2030 is refused (more than 30 days ahead); the retry example reads as if it applied to the cancelled id. Fixed in the README.

Earlier delivery findings 2 to 6: closed, by reading the code.
