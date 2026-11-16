# Contract digest (frozen 13 Nov 2026)

Facts every lane builds against. No verdicts here; reasons are in `docs/decisions/`. The decision records are still `proposed`; this run builds to their recommended options (owner item 0001).

## Sources of truth
- HTTP: the Huma operations in `internal/httpapi` (`api.go`, `contract.go`); `api/openapi.yaml` (v2.0.0) is generated from them (decision 0024), and clients generate from it. Schema: `schema/notifications.sql`. Vocabulary, state machine, limits, ports: `internal/notification/` (`status.go`, `limits.go`, `ports.go`).
- Only the coordinator edits those. A lane that needs a change writes `.agents/owner-items/NNNN-<slug>.md`, takes the nearest assumption that needs no contract change, and continues.
- Generated, never hand-edited: `api/openapi.yaml`, `test/apiclient/client.gen.go`, `internal/store/sqlc/`, `migrations/`. Regenerate with `go tool -modfile=tools.mod task gen`.

## Process roles (decision 0002)
One binary, `cmd/app`: `app migrate`, `app serve` (HTTP API), `app worker` (delivery). Any number of each may run against one MySQL and one Valkey. `app healthcheck` exists for the container probe.

## HTTP conventions
- Refusals use the `Error` body `{code, message, details[{index?, field, reason, ...}]}`. A detail may carry more fields for its kind of refusal; `field`, `reason` and `index` keep their meaning everywhere.
- 400 `invalid_request` for every broken rule, whether Huma's request validation or the service found it; `details[].field` names the JSON field (`to`, `content`, `template.variables`, `template.variables.<name>` for a value over its length, `scheduledAt`, `notifications`), or the header (`Idempotency-Key`, `If-Match`), `index` the batch position.
- Missing template variables (decision 0015): one detail per request or batch item, `field: "template.variables"`, with `required` (the template's variables in order of first appearance), `missing` (order of first appearance) and `unknown` (sent but unused, sorted, `[]` when none). Unknown variables alone are accepted.
- Templates have `version` (1, then +1 per update) in the body and as a strong `ETag` (`"3"`). `PUT /v1/templates/{name}`: no `If-Match` creates (201) or, if the template exists, 428 `precondition_required`; `If-Match` at the current version replaces (200); a stale version or a missing template is 412 `precondition_failed`. Both carry `currentVersion` on the `If-Match` detail when the template exists.
- A notification rendered from a template shows `template: {name, version}` (read-only); absent for plain content.
- 413 `request_too_large` for a body over 8 MiB. 404 `not_found`. 409 `not_cancellable` (cancel when not pending), 409 `not_retryable` (retry when not failed), 409 `not_reschedulable` (PATCH reschedule once a worker has picked it up; the message names the status). 412 and 428 as above. 422 `idempotency_key_reused`.
- `X-Correlation-ID`: accepted, else generated; echoed on every response; stored on the row; on every log line about that notification in API and worker (JSON logs on stdout, field `correlation_id`; `notification_id` when known); sent to the provider as `X-Correlation-ID`.
- `Idempotency-Key` on POST create (single or batch): same key and same payload returns the first response, same ids, status 201, header `Idempotent-Replayed: true`. Same key, different payload: 422.
- Outside the Huma operations (not validated by Huma): `GET /v1/ws` (WebSocket, `github.com/coder/websocket`; documented in the spec, its message schema `StatusEvent` from `contract.go`), `GET /openapi.json` (the spec built from the operations), `GET /docs` (a page that renders it).
- Status shown by the API: stored status, except `scheduled` when stored status is `pending`, `attempts = 0` and `due_at` is in the future.

## Provider call (R16, decisions 0006, 0019)
`POST <channel's provider URL>` with JSON `{"to","channel","content"}` and headers `Idempotency-Key: <notification id>` (identical across attempts), `X-Correlation-ID`, `traceparent`. Expected `202 {"messageId","status","timestamp"}`; `messageId` is stored. The provider double is `internal/fakeprovider` (`go doc ./internal/fakeprovider`); tests start it with `support.Provider(t)`.

## Delivery rules
- Classify (only in `provider.Classify`): retry on network error, timeout, 408, 425, 429, 5xx; permanent on any other 4xx; a 2xx without `messageId` counts as sent.
- Backoff: `random(0, min(cap, base * 2^attempt))`, never shorter than `Retry-After`; stored as `due_at`. After `APP_MAX_ATTEMPTS` counted failures (`budget_spent`; a failure during a declared outage is recorded but not counted, decision 0026): `failed`, `failure_kind = exhausted`. Permanent: `failed`, `permanent`, after one call.
- Rate limit: GCRA in Valkey with burst 1, Valkey's clock, one key per channel. `Limiter.Take` reserves consecutive send slots one interval apart, a free grid starting a lead ahead (the worker's measured claim time, p95 of its newest 20 claims, between 200 ms (the fixed alignment it replaced) and an eighth of the lease) and none further than lead plus horizon (`APP_RATE_LIMIT_HORIZON` or the lead, whichever is larger); slots are reserved before claiming, one claim query serves a group of slots, and each request leaves at its slot, never earlier, except that a group sent after an idle grid leaves as soon as it is claimed; slots a claim found no row for go back to the limiter (`Limiter.GiveBack`): at once when nobody reserved after them, otherwise once everything reserved after them has gone back too. A job that misses its slot by more than one interval is released, not sent late. Valkey unreachable: workers send nothing and `/readyz` says so.
- A request is sent only if its lease outlives the call: the call's timeout is capped at the time left on the lease minus a margin, and a job with too little lease left is released.
- Priority: per-channel permits are split by weights with borrowing (unused share goes to the next priority down, then up). One claim query per priority through `claim_idx`.
- Provider health per channel, shared by all workers in Valkey (decision 0026): `APP_BREAKER_FAILURES` consecutive provider faults declare an outage; one probe per cool-down fleet-wide; while open no worker claims that channel and no failure counts against a message's budget.
- Lease: claim sets `lease_until` and increments `claim_version`; every later write carries `WHERE claim_version = ?`; a reaper returns expired leases to `pending`.
- Claim transaction: READ COMMITTED, `FOR UPDATE SKIP LOCKED`, `ORDER BY due_at` matching `claim_idx`; `due_at <= NOW(3)` on the database clock.
- Events: every committed status change is published once through `core.Publisher` (Valkey pub/sub). Publishing never delays a response by more than one bounded wait per request, whatever the number of events (a batch is one pipelined publish with one deadline). When an API instance loses its event feed it closes its WebSocket streams and refuses new ones until the feed is back.

## Settings (all in `config`, each with default, bounds, doc line)
Each dial's doc line ends with where its default comes from (`go run ./cmd/app env`); the derivations and the measurements behind them are in decision 0018, "As built". `config.Load` and `app env check` refuse combinations that cannot work (`config/cross_checks.go`): retry cap below base, notification retention below key retention, a lease shorter than provider timeout + longest slot wait + one slot interval + 500 ms margin, and a shutdown grace shorter than the provider or request timeout.

| Variable | Default | Meaning | Reason |
|---|---|---|---|
| `APP_HTTP_ADDR` | `:8080` | listen address | conventional unprivileged HTTP port |
| `APP_DATABASE_DSN` | required | MySQL DSN | a secret, injected at runtime |
| `APP_VALKEY_ADDR` | `localhost:6379` | Valkey host:port | Valkey's standard port |
| `APP_OTLP_ENDPOINT` | empty | OTLP/HTTP base URL; empty disables export | a bare run needs no collector; compose sets port 4318 |
| `APP_SERVICE_NAME` | `notifier` | `service.name` on all telemetry | free choice; OpenTelemetry requires one |
| `APP_LOG_LEVEL` | `info` | log level | slog's default |
| `APP_DB_MAX_OPEN_CONNS` | `10` | MySQL pool per process | 3 claims + 1 reap + writes = 5 needed, doubled; 24 measured worse on a slow disk |
| `APP_READ_HEADER_TIMEOUT` | `5s` | time to send request headers | one round trip is under 1 s; far under nginx's 60 s |
| `APP_REQUEST_TIMEOUT` | `10s` | answered 503 after | an 8 MiB batch at 10 Mbit/s (6.7 s) + insert + 1 s publish budget, rounded up |
| `APP_SHUTDOWN_TIMEOUT` | `10s` | grace on SIGTERM | at least the request and provider timeouts (enforced); + 5 s final writes + 5 s flush + 5 s spare = compose's 25 s stop grace |
| `APP_PROVIDER_URL_SMS`, `_EMAIL`, `_PUSH` | `http://localhost:8090/send` | provider URL per channel | the provider double's local address |
| `APP_PROVIDER_TIMEOUT` | `5s` | one provider call | 10 × the assumed 500 ms provider p99 |
| `APP_MAX_ATTEMPTS` | `10` | counted failures before `exhausted` | the smallest budget whose waits reach the cap, ceil(log2(cap/base)) + 1; a message-specific failure retries for about 6.75 min on average, 13.5 at most (decision 0026; an outage no longer spends budget) |
| `APP_RETRY_BASE` | `1s` | backoff base | decision 0007: full-jitter backoff, past a transient blip |
| `APP_RETRY_CAP` | `5m` | backoff cap | decision 0007 |
| `APP_RATE_LIMIT_PER_SECOND` | `100` | provider's limit per channel | the brief |
| `APP_RATE_LIMIT_HEADROOM_PERCENT` | `5` | the limiter paces this much under the limit, so arrival jitter cannot show 101 in a second at the provider | measured (owner item 0013): at 95 the busiest second is 98 to 99 |
| `APP_RATE_LIMIT_HORIZON` | `200ms` | least span of send slots one limiter call reserves past the lead | a budget of 10 limiter calls per second per channel per worker |
| `APP_PRIORITY_WEIGHTS` | `70,20,10` | high, normal, low shares | free choice (decision 0010); tune to the traffic mix |
| `APP_LEASE_DURATION` | `30s` | claim lease | an eighth is the alignment's ceiling, 3.75 s, above a loaded database's claims (about 3 s at p95, owner item 0011); floor 7.35 s at the default timeout (enforced) |
| `APP_REAP_INTERVAL` | `5s` | how often expired leases are returned | crash recovery is lease + reap = 35 s; shorter adds little next to the 30 s lease |
| `APP_WORKER_CONCURRENCY` | `72` | provider calls in flight per worker process | Little's law: 2 of 3 workers carry 285/s at a 500 ms p99, 285 × 0.5 / 2 |
| `APP_BREAKER_FAILURES` | `5` | consecutive provider faults, fleet-wide, that declare an outage | Envoy outlier detection's consecutive_5xx |
| `APP_BREAKER_COOLDOWN` | `5s` | least time before the next probe, fleet-wide | Hystrix's default sleep window; one call per 5 s while down |
| `APP_IDEMPOTENCY_RETENTION` | `24h` | how long a key is remembered | Stripe's 24 h key window |
| `APP_IDEMPOTENCY_SWEEP_INTERVAL` | `1m` | how often expired keys and expired finished notifications are deleted | expired keys linger at most 0.07% of a 24 h table |
| `APP_NOTIFICATION_RETENTION` | `168h` | how long finished notifications and their attempts are kept (1h..365d, at least the key retention; decision 0025) | a Friday failure is still replayable on Monday |
| `APP_WS_SEND_BUFFER` | `1024` | events buffered per subscriber before it is dropped | one batch create publishes up to 1000 events at once; about 140 KB per socket |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | pass-through, read by the OpenTelemetry SDK and validated by `config`; production `parentbased_traceidratio` (decision 0019) | the SDK's own default |
| `OTEL_TRACES_SAMPLER_ARG` | `1` | pass-through; share of new traces kept by the `*traceidratio` samplers; production `0.02`, derived in decision 0019 | the SDK's own default |

## Acceptance test harness (`test/acceptance/`, build tag `integration`)
- Black box: the tests build `./cmd/app` once (`TestMain`), start real MySQL and Valkey with `test/support` (`support.MySQL(t)` returns a migrated database and its DSN, `support.Valkey(t)` an address, `support.Provider(t)` the double's URL and handle), then run `app serve` and N × `app worker` as child processes configured only through the variables above. They speak HTTP to the API, read the provider double's counters, and read the children's stdout for log assertions.
- Run: `go tool -modfile=tools.mod task test:integration -- ./test/acceptance/...` or `go test -tags integration ./test/acceptance -run <Test>`.
- Each test names its requirement row: `// R5: cancel pending notifications`.
- Nothing under `test/acceptance/` is changed by an implementer. A test believed wrong is reported in the hand-back with the evidence.
