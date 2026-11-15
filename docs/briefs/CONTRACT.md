# Contract digest (frozen 13 Nov 2026)

Facts every lane builds against. No verdicts here; reasons are in `docs/decisions/`. The decision records are still `proposed`; this run builds to their recommended options (owner item 0001).

## Sources of truth
- HTTP: the Huma operations in `internal/httpapi` (`api.go`, `contract.go`); `api/openapi.yaml` (v1.3.0) is generated from them (decision 0024), and clients generate from it. Schema: `schema/notifications.sql`. Vocabulary, state machine, limits, ports: `internal/notification/` (`status.go`, `limits.go`, `ports.go`).
- Only the coordinator edits those. A lane that needs a change writes `.agents/owner-items/NNNN-<slug>.md`, takes the nearest assumption that needs no contract change, and continues.
- Generated, never hand-edited: `api/openapi.yaml`, `test/apiclient/client.gen.go`, `internal/store/sqlc/`, `migrations/`. Regenerate with `go tool -modfile=tools.mod task gen`.

## Process roles (decision 0002)
One binary, `cmd/app`: `app migrate`, `app serve` (HTTP API), `app worker` (delivery). Any number of each may run against one MySQL and one Valkey. `app healthcheck` exists for the container probe.

## HTTP conventions
- Refusals use the `Error` body `{code, message, details[{index?, field, reason}]}`.
- 400 `invalid_request` for every broken rule, whether Huma's request validation or the service found it; `details[].field` names the JSON field (`to`, `content`, `template.variables.name`, `scheduledAt`, `notifications`), `index` the batch position.
- 413 `request_too_large` for a body over 8 MiB. 404 `not_found`. 409 `not_cancellable` (cancel when not pending), 409 `not_retryable` (retry when not failed). 422 `idempotency_key_reused`.
- `X-Correlation-ID`: accepted, else generated; echoed on every response; stored on the row; on every log line about that notification in API and worker (JSON logs on stdout, field `correlation_id`; `notification_id` when known); sent to the provider as `X-Correlation-ID`.
- `Idempotency-Key` on POST create (single or batch): same key and same payload returns the first response, same ids, status 201, header `Idempotent-Replayed: true`. Same key, different payload: 422.
- Outside the Huma operations (not validated by Huma): `GET /v1/ws` (WebSocket, `github.com/coder/websocket`; documented in the spec, its message schema `StatusEvent` from `contract.go`), `GET /openapi.json` (the spec built from the operations), `GET /docs` (a page that renders it).
- Status shown by the API: stored status, except `scheduled` when stored status is `pending`, `attempts = 0` and `due_at` is in the future.

## Provider call (R16, decisions 0006, 0019)
`POST <channel's provider URL>` with JSON `{"to","channel","content"}` and headers `Idempotency-Key: <notification id>` (identical across attempts), `X-Correlation-ID`, `traceparent`. Expected `202 {"messageId","status","timestamp"}`; `messageId` is stored. The provider double is `internal/fakeprovider` (`go doc ./internal/fakeprovider`); tests start it with `support.Provider(t)`.

## Delivery rules
- Classify (only in `provider.Classify`): retry on network error, timeout, 408, 425, 429, 5xx; permanent on any other 4xx; a 2xx without `messageId` counts as sent.
- Backoff: `random(0, min(cap, base * 2^attempt))`, never shorter than `Retry-After`; stored as `due_at`. After `APP_MAX_ATTEMPTS` counted failures (`budget_spent`; a failure during a declared outage is recorded but not counted, decision 0026): `failed`, `failure_kind = exhausted`. Permanent: `failed`, `permanent`, after one call.
- Rate limit: GCRA in Valkey with burst 1, Valkey's clock, one key per channel. `Limiter.Take` reserves consecutive send slots one interval apart, at most `APP_RATE_LIMIT_HORIZON` ahead; slots are reserved before claiming, one claim query serves a group of slots, and each request leaves at its slot, never earlier. A job that misses its slot by more than one interval is released, not sent late. Valkey unreachable: workers send nothing and `/readyz` says so.
- A request is sent only if its lease outlives the call: the call's timeout is capped at the time left on the lease minus a margin, and a job with too little lease left is released.
- Priority: per-channel permits are split by weights with borrowing (unused share goes to the next priority down, then up). One claim query per priority through `claim_idx`.
- Provider health per channel, shared by all workers in Valkey (decision 0026): `APP_BREAKER_FAILURES` consecutive provider faults declare an outage; one probe per cool-down fleet-wide; while open no worker claims that channel and no failure counts against a message's budget.
- Lease: claim sets `lease_until` and increments `claim_version`; every later write carries `WHERE claim_version = ?`; a reaper returns expired leases to `pending`.
- Claim transaction: READ COMMITTED, `FOR UPDATE SKIP LOCKED`, `ORDER BY due_at` matching `claim_idx`; `due_at <= NOW(3)` on the database clock.
- Events: every committed status change is published once through `core.Publisher` (Valkey pub/sub). Publishing never delays a response by more than one bounded wait per request, whatever the number of events (a batch is one pipelined publish with one deadline). When an API instance loses its event feed it closes its WebSocket streams and refuses new ones until the feed is back.

## Settings (all in `config`, each with default, bounds, doc line)
| Variable | Default | Meaning |
|---|---|---|
| `APP_HTTP_ADDR` | `:8080` | exists |
| `APP_DATABASE_DSN` | required | exists |
| `APP_VALKEY_ADDR` | `localhost:6379` | exists |
| `APP_PROVIDER_URL_SMS`, `_EMAIL`, `_PUSH` | `http://localhost:8090/send` | provider URL per channel |
| `APP_PROVIDER_TIMEOUT` | `5s` | one provider call |
| `APP_MAX_ATTEMPTS` | `10` | counted failures before `exhausted`: the smallest budget whose waits reach the cap; a message-specific failure retries for about 6.75 min on average, 13.5 at most (decision 0026; an outage no longer spends budget) |
| `APP_RETRY_BASE` | `1s` | backoff base |
| `APP_RETRY_CAP` | `5m` | backoff cap |
| `APP_RATE_LIMIT_PER_SECOND` | `100` | provider's limit per channel |
| `APP_RATE_LIMIT_HEADROOM_PERCENT` | `3` | the limiter paces this much under the limit, so arrival jitter cannot show 101 in a second at the provider |
| `APP_RATE_LIMIT_HORIZON` | `200ms` | how far ahead send slots may be reserved |
| `APP_PRIORITY_WEIGHTS` | `70,20,10` | high, normal, low shares |
| `APP_LEASE_DURATION` | `30s` | claim lease |
| `APP_REAP_INTERVAL` | `5s` | how often expired leases are returned |
| `APP_WORKER_CONCURRENCY` | `32` | provider calls in flight per worker process |
| `APP_BREAKER_FAILURES` | `5` | consecutive provider faults, fleet-wide, that declare an outage |
| `APP_BREAKER_COOLDOWN` | `5s` | least time before the next probe, fleet-wide |
| `APP_IDEMPOTENCY_RETENTION` | `24h` | how long a key is remembered |
| `APP_IDEMPOTENCY_SWEEP_INTERVAL` | `1m` | how often expired keys are deleted |
| `APP_WS_SEND_BUFFER` | `256` | events buffered per subscriber before it is dropped |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | pass-through, read by the OpenTelemetry SDK and validated by `config`; production `parentbased_traceidratio` (decision 0019) |
| `OTEL_TRACES_SAMPLER_ARG` | `1` | pass-through; share of new traces kept by the `*traceidratio` samplers; production `0.02`, derived in decision 0019 |

## Acceptance test harness (`test/acceptance/`, build tag `integration`)
- Black box: the tests build `./cmd/app` once (`TestMain`), start real MySQL and Valkey with `test/support` (`support.MySQL(t)` returns a migrated database and its DSN, `support.Valkey(t)` an address, `support.Provider(t)` the double's URL and handle), then run `app serve` and N × `app worker` as child processes configured only through the variables above. They speak HTTP to the API, read the provider double's counters, and read the children's stdout for log assertions.
- Run: `go tool -modfile=tools.mod task test:integration -- ./test/acceptance/...` or `go test -tags integration ./test/acceptance -run <Test>`.
- Each test names its requirement row: `// R5: cancel pending notifications`.
- Nothing under `test/acceptance/` is changed by an implementer. A test believed wrong is reported in the hand-back with the evidence.
