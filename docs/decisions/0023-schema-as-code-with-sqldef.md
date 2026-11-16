---
status: accepted
date: 2026-11-15
decision-makers: owner (in chat)
revisit: when a trigger or stored function is needed (sqldef cannot parse SIGNAL)
---
# schema/ is the desired state, diffed into goose migrations by sqldef (0023)

## Rule
Edit `schema/**/*.sql` (one file per aggregate: a weak table and its triggers sit with their owner; files are read in path order, so a referenced table sorts first), then run `task migrate:diff -- <name>`; never hand-write `migrations/`. `task migrate:check` fails when they disagree.

## Context
Owner, 15 Nov 2026: "atlas not supporting subdirectories is straight up BS ... requires you to pay ... Every source being colocated is useful. Its stupid to switch files or directories to see code for the table and a trigger action upon it"; "we obviously not gonna pay atlas so what alternative is there". Atlas Community reads a schema folder flat (top-level files only), rejects `atlas:import`, and manages triggers, functions and views only after `atlas login`.

## Options
1. Atlas Community. Worked for tables; no nested folders, no triggers.
2. sqldef (`mysqldef`, MIT, Go). Diffs any number of SQL files against a live database or a file, supports MySQL triggers and views, `--check` exits 2 on drift; no migration history of its own.
3. Skeema. MySQL only, one file per object; applies changes directly instead of writing versioned migrations; triggers are in its paid tier.
4. Hand-written goose migrations. No tool; every change written twice (schema and migration) or the desired state is lost.

## Decision
Option 2. `tools/schemadiff` concatenates `schema/`, diffs it against a scratch MySQL that has `migrations/` applied (Up), applies Up there and diffs the two exports (Down), and writes the goose file; sqlc keeps reading `migrations/`. Pinned in `tools.mod`, so nothing beyond Go and Docker is needed. Measured on this schema (v3.11.30): parses every table unchanged, idempotent, triggers with `BEGIN ... END` bodies round-trip (a changed body is `DROP` + `CREATE`), column and index changes come out as plain `ALTER`s. Two caveats: a CHECK that compares a column with a string must be written as MySQL stores it (`SHOW CREATE TABLE`), or sqldef reports a false change; and its parser rejects `SIGNAL`, so a trigger that refuses a write cannot be expressed. No triggers are used: rules stay in the Go state machine (decision 0005).

## Confirmation
`task migrate:check` (CI job "static: lint, migration drift").

## As built: the first triggers (15 Nov 2026)
Decision 0005's database rules are the first triggers. The refusal is a CHECK fed by the triggers (sqldef still cannot parse `SIGNAL`), and three gaps showed up and are closed in `tools/schemadiff`: mysqldef never emits `DROP TRIGGER` for a MySQL trigger that `schema/` no longer declares (so Down, Up and `-check` now compare trigger names themselves); goose splits a trigger body on its semicolons (each `CREATE TRIGGER` is wrapped in `StatementBegin`/`StatementEnd`); and sqlc's MySQL parser rejects `CREATE TRIGGER`, so sqlc reads `internal/store/schema.gen.sql`, the migrated schema without triggers, written by `schemadiff -name` and checked by `-check`.
