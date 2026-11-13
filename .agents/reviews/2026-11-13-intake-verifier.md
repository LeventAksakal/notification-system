# Intake slice: fresh-context verifier report (13 Nov 2026, Opus, on `pr/a-intake` 74d9cbf)

Verdict: pass with findings. Proofs: acceptance 20/20 in 72.8 s; `go test -race ./internal/... ./cmd/...` pass; `go vet ./...` pass. None of the findings was reproduced live (the guard blocked the verifier's probe clients); all are inferred from the code.

| # | Severity | Finding | Where | Status |
|---|---|---|---|---|
| 1 | Medium | A replay with the same Idempotency-Key is refused (400) if its template was deleted or changed in between, because templates are loaded and validated before the key is looked up | `internal/intake/service.go:105-112`, `:151-169` | queued: intake fix-up |
| 2 | Medium | A non-ASCII Idempotency-Key gives 500: the spec allows any string, the column is ASCII | `api/openapi.yaml` IdempotencyKey, `schema/notifications.sql` idem_key | queued: contract (pattern) + regenerate |
| 3 | Low | List filters on status or a date range scan from the newest row down; no index gives id order for them | `internal/store/queries/intake.sql:35-44` | queued: `list_status_idx`, date range as id bounds |
| 4 | Low | Batch counts and items are read in separate statements and can disagree for a moment | `internal/intake/service.go:424-434` | accepted, documented |
| 5 | Low | No request body size limit | `internal/api/middleware.go` | queued: limit in `core`, 413 |

Test gaps named by the verifier: R5 cancel of a sent or processing notification (delivery tests cover the race); R6 filter combined with a cursor, and the sent / failed / processing filters; R11 template change between replays, same key across single and batch; R2 a database failure in the second chunk; D4 spec matches handlers only by construction (generated strict server); D5 drift is checked by `task migrate:check` in CI, not by an acceptance test.
