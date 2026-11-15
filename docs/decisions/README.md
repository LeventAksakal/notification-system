# Decisions in force

Accepted decisions only; this list is what agents load. Superseded records move to `superseded/` with a banner pointing at their replacement. `go test ./tools/adrcheck` enforces both.

Record 0004 in this folder is still `proposed` (the owner rejected it on 15 Nov 2026; record 0024, code-first with Huma, replaces it when that branch merges), so it is not listed here. Each was written before its slice was built; the code follows the recommended option, and where it does not the record says so under "As built". Accepting them is the owner's step. `docs/REQUIREMENTS.md` names the record behind each requirement.

- [Record decisions as short, checkable ADRs (0001)](0001-record-decisions-as-adrs.md)
- [One image runs as api, worker or migrate; workers scale by replica (0002)](0002-one-image-three-roles.md)
- [MySQL 8.4 is the system of record and the queue (0003)](0003-mysql-is-record-and-queue.md)
- [A notification moves through one explicit state machine and every attempt is recorded (0005)](0005-one-state-machine-attempts-recorded.md)
- [Delivery is at-least-once, with an idempotency key sent to the provider (0006)](0006-at-least-once-with-provider-key.md)
- [Retries are classified, backed off with full jitter, and stored as a due time (0007)](0007-retries-classified-jittered-stored.md)
- [A per-channel circuit breaker pauses claiming, so an outage burns no attempts (0008)](0008-breaker-pauses-claiming.md)
- [The per-channel limit is global, enforced in Valkey before rows are claimed (0009)](0009-global-rate-limit-in-valkey.md)
- [Priority is weighted, not strict: high goes first, low never starves (0010)](0010-priority-is-weighted.md)
- [A batch is accepted or rejected as a whole (0011)](0011-batch-is-atomic.md)
- [Client retries are deduplicated by an Idempotency-Key header (0012)](0012-client-retries-deduplicated-by-key.md)
- [Live status updates use WebSocket, fanned out through Valkey pub/sub (0013)](0013-websocket-with-valkey-fanout.md)
- [The metrics endpoint is a JSON summary read from shared state; OpenTelemetry carries the time series (0014)](0014-metrics-json-summary-and-otel.md)
- [Templates are rendered and validated when the notification is accepted (0015)](0015-templates-rendered-at-intake.md)
- [Scheduling is a future due time on the queue row; no scheduler process (0016)](0016-scheduling-is-a-due-time.md)
- [Each content limit is one constant, enforced in the spec, the service and the schema (0017)](0017-limits-are-single-constants.md)
- [Built for many instances on one database: claims are leases with a fencing token (0018)](0018-many-instances-one-database.md)
- [One correlation id and one trace follow a notification across the queue (0019)](0019-one-correlation-id-one-trace.md)
- [One settings schema in code; secret values come from the platform, never from the repository (0020)](0020-one-settings-schema-secrets-from-the-platform.md)
- [main moves by fast-forward; releases are cut by hand from a green commit (0021)](0021-fast-forward-main-releases-cut-by-hand.md)
- [Packages are named for their role; contracts and settings sit at the root (0022)](0022-packages-by-role-contracts-at-the-root.md)
- [schema/ is the desired state, diffed into goose migrations by sqldef (0023)](0023-schema-as-code-with-sqldef.md)
