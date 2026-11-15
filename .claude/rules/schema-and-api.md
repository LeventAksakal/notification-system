---
paths: ["schema/**", "migrations/**", "api/**", "internal/store/**", "internal/httpapi/**"]
---
# Contracts: schema and API

- `schema/**/*.sql` is the desired end state, one file per aggregate (a weak table and its triggers with their owner; files read in path order). Change it, then `task migrate:diff -- <name>`; sqldef writes the goose migration. `task migrate:check` fails on drift.
- Queries live in `internal/store/queries/*.sql`; `task gen` turns them into typed Go via sqlc. Write SQL, not an ORM.
- Every table: explicit primary key, `CHECK`s for domain limits, indexes named for the query they serve (`claim_idx`).
- The HTTP contract is the server's code (decision 0024): the Huma operations in `internal/httpapi/api.go` and the types in `contract.go`, whose limits come from `internal/notification/limits.go`. `task gen` writes `api/openapi.yaml` from them and the Go client in `test/apiclient` from the spec; never hand-edit either. CI fails on a stale spec and on a breaking change against the base (`task api:breaking`).
- These paths are contracts (`.claude/agent-policy.json`): only the coordinator edits them; a lane whose diff regenerates `api/openapi.yaml` fails the merge ownership check. An implementer needing a change writes an owner item.
