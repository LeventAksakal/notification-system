---
paths: ["schema/**", "migrations/**", "api/**", "internal/store/**"]
---
# Contracts: schema and API

- `schema/*.sql` is the desired end state. Change it, then `task migrate:diff -- <name>`; Atlas writes the goose migration. `task migrate:check` fails on drift.
- Queries live in `internal/store/queries/*.sql`; `task gen` turns them into typed Go via sqlc. Write SQL, not an ORM.
- Every table: explicit primary key, `CHECK`s for domain limits, indexes named for the query they serve (`claim_idx`).
- `api/openapi.yaml` is the API contract; `task gen` turns it into the strict server interface. Handlers that drift from the spec do not compile.
- These paths are contracts: only the coordinator edits them. An implementer needing a change writes an owner item.
