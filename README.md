# Notification system

Accepts notification requests for SMS, email and push, queues them, and delivers them to an external provider at no more than 100 messages per second per channel, with priorities, retries, a circuit breaker, scheduling, templates, status tracking and live status events.

Go 1.27, MySQL 8.4 (the record and the queue), Valkey 9 (the shared rate limit and event fan-out), OpenTelemetry.

## Run it (needs only Docker)

```bash
docker compose up --build --wait
```

That starts MySQL, Valkey, the API, three delivery workers, a local stand-in for the provider, and Grafana. Nothing else to install or configure.

| What | Where |
|---|---|
| API reference (rendered OpenAPI) | http://localhost:8080/docs |
| OpenAPI document | http://localhost:8080/openapi.json (source: [`api/openapi.yaml`](api/openapi.yaml)) |
| Readiness | http://localhost:8080/readyz |
| Metrics | http://localhost:8080/v1/metrics |
| Provider stand-in: what it received | http://localhost:8090/_control/stats |
| Grafana: traces, logs, metrics | http://localhost:3000 |

`docker compose down` stops it; add `-v` to drop the database volume. If a port is taken, set `APP_PORT`, `PROVIDER_PORT` or `GRAFANA_PORT`.

## Try it

Send one notification and read its status:

```bash
curl -s -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1001-sms' \
  -H 'X-Correlation-ID: demo-1' \
  -d '{"to":"+905551234567","channel":"sms","content":"Your code is 4821","priority":"high"}'
```

```json
{"id":"0193…","to":"+905551234567","channel":"sms","priority":"high","status":"pending","attempts":0,"correlationId":"demo-1","…":"…"}
```

```bash
curl -s localhost:8080/v1/notifications/<id>
```

A moment later the status is `sent`, with the provider's message id and one entry in `deliveryAttempts`.

A batch of up to 1000, stored all or nothing:

```bash
curl -s -X POST localhost:8080/v1/notifications/batch \
  -H 'Content-Type: application/json' \
  -d '{"notifications":[
        {"to":"ada@example.com","channel":"email","content":"Welcome"},
        {"to":"device-token-1","channel":"push","content":"Sale starts now","priority":"low"}]}'
curl -s localhost:8080/v1/batches/<batchId>
```

A template, and a notification scheduled for later that uses it:

```bash
curl -s -X PUT localhost:8080/v1/templates/welcome \
  -H 'Content-Type: application/json' -d '{"body":"Hello {{name}}, welcome aboard."}'
curl -s -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"to":"ada@example.com","channel":"email","template":{"name":"welcome","variables":{"name":"Ada"}},"scheduledAt":"2030-01-01T09:00:00Z"}'
```

Cancel it while it waits, list with filters, replay a dead letter, read the numbers:

```bash
curl -s -X POST localhost:8080/v1/notifications/<id>/cancel
curl -s 'localhost:8080/v1/notifications?status=failed&channel=sms&limit=20'
curl -s -X POST localhost:8080/v1/notifications/<id>/retry
curl -s 'localhost:8080/v1/metrics?windowSeconds=60'
```

Live status events over WebSocket (every change, or one notification, or one batch):

```bash
npx wscat -c 'ws://localhost:8080/v1/ws?notificationId=<id>'
```

Make the provider fail and watch retries and the breaker (the stand-in's control API):

```bash
curl -s -X PUT localhost:8090/_control/faults -H 'Content-Type: application/json' \
  -d '{"rules":[{"channel":"sms","mode":"status","status":503,"count":50}]}'
curl -s -X DELETE localhost:8090/_control/faults
```

### Endpoints

| Method and path | What it does |
|---|---|
| `POST /v1/notifications` | Queue one notification (`to`, `channel`, `content` or `template`, `priority`, `scheduledAt`) |
| `POST /v1/notifications/batch` | Queue 1 to 1000 atomically |
| `GET /v1/notifications/{id}` | Status, attempt count, last error, attempt history |
| `GET /v1/notifications` | List, newest first; filters `status`, `channel`, `batchId`, `createdFrom`, `createdTo`; `limit` and `cursor` |
| `POST /v1/notifications/{id}/cancel` | Cancel while pending (including scheduled or waiting to retry) |
| `POST /v1/notifications/{id}/retry` | Replay a failed notification (dead letter) |
| `GET /v1/batches/{id}` | Counts per status and the batch's items |
| `PUT`, `GET`, `DELETE /v1/templates/{name}`, `GET /v1/templates` | Templates with `{{variable}}` placeholders |
| `GET /v1/metrics` | Queue depth, success and failure rates, latency percentiles |
| `GET /v1/ws` | WebSocket stream of status changes |
| `GET /healthz`, `GET /readyz` | Liveness; readiness of MySQL and Valkey |

Refusals share one body: `{"code","message","details":[{"index","field","reason"}]}`. 400 names each offending field (and the item index in a batch); 409 means the notification is not in a state that allows the action; 413 is a body over 8 MiB; 422 is an `Idempotency-Key` reused with a different payload.

Limits, defined once in [`internal/core/limits.go`](internal/core/limits.go) and enforced by the spec, the service and database `CHECK`s: SMS `to` is E.164 and content is 1 to 1600 characters; email `to` is at most 254 characters and content 1 to 100,000; push `to` is a device token of 1 to 255 characters and content 1 to 1024.

## How it works

```mermaid
flowchart LR
  client([API client]) -->|REST, WebSocket| api["app serve<br/>(any number)"]
  api -->|one transaction per request| db[(MySQL<br/>record and queue)]
  w["app worker × 3<br/>(any number)"] -->|claim: FOR UPDATE SKIP LOCKED,<br/>lease + claim_version| db
  w -->|reserve send slots| vk[(Valkey<br/>rate limit, events)]
  w -->|"POST {to, channel, content}<br/>Idempotency-Key = notification id"| provider([Provider])
  w -->|status events| vk
  api -->|subscribe| vk
  api & w -->|traces, logs, metrics| otel[OpenTelemetry → Grafana]
```

One binary, three roles: `app migrate`, `app serve`, `app worker`. No process assumes it is the only one.

- **Accepting.** A request is validated (spec, then service rules), templates are rendered, and the notification is stored in one transaction; a batch and its items are one transaction. The API answers `201` before anything is sent. An `Idempotency-Key` is stored in the same transaction, so the primary key decides the race between two identical requests.
- **The queue is the table.** A notification is `pending` with a due time. Workers claim due rows with `SELECT … FOR UPDATE SKIP LOCKED` under READ COMMITTED through an index that matches the query's order, so claimers never wait for each other. A scheduled notification is simply a row whose due time is in the future; a retry is the same row with a later due time. Nothing sleeps holding a message.
- **One state machine.** `pending → processing → sent | failed`, `pending → cancelled`, `processing → pending` (retry or expired lease), `failed → pending` (replay). Every change is a conditional `UPDATE … WHERE status = <from>`, so a cancel racing a worker has exactly one winner. Each provider call is one row in `delivery_attempts`.
- **Rate limit.** A GCRA limiter in Valkey, on Valkey's clock, hands out send slots spaced one interval apart across all workers. A worker reserves the next slots, claims that many rows in one query, and sends each at its slot. Slots are reserved before rows are claimed, and a job that misses its slot is released, never sent late. The limiter paces 3% under the limit so arrival jitter cannot show the provider 101 in a second. If Valkey is unreachable the workers send nothing and readiness says so.
- **Priority.** Each channel's slots are split between high, normal and low by weights (70 / 20 / 10). A priority that has nothing due lends its slots to the next one, so high traffic gets everything when alone and low traffic always moves.
- **Retries.** One classifier decides: network errors, timeouts, 408, 425, 429 and 5xx are retried with full-jitter exponential backoff, never sooner than `Retry-After`; any other 4xx fails at once. After five calls a notification is `failed` (`exhausted`): the dead letter, listed with `status=failed` and replayable.
- **Circuit breaker.** Per channel, in each worker. While it is open the worker does not claim that channel's rows, so an outage burns no attempts and other channels keep their rate.
- **Crashes.** A claim is a lease with a version. A worker that dies leaves rows that a reaper returns to `pending`; a worker that was paused past its lease does not send (each call is capped by the lease's remaining time) and its late write changes nothing (`WHERE claim_version = ?`).
- **Events.** Every committed status change is published once through Valkey; `GET /v1/ws` streams them from any API instance. REST stays the source of truth.
- **Observability.** JSON logs carry the notification's correlation id in the API and the workers, and it is sent to the provider. The delivery span continues the accepting request's trace, so one trace covers the request, the queue wait and the provider call. `GET /v1/metrics` is computed from MySQL, so every instance gives the same numbers.

### What "sent once" means here

Delivery is at-least-once. Duplicates are prevented on our side: client retries by `Idempotency-Key`, concurrent workers by the row lock and the lease version. A send whose outcome is unknown (a timeout after the provider accepted) is retried with the same `Idempotency-Key`, so it is delivered once only if the provider honours that key. The provider stand-in implements key dedupe as a switch (`PUT /_control/dedupe`), and the tests measure both cases.

### What comes next at larger scale

One MySQL node carries this design to the low thousands of messages per second. Beyond that: shard by recipient hash, partition the tables by day and drop old partitions, and share breaker state between workers. The queue sits behind `core.Queue`, so a broker can replace the table without touching the workers' logic.

## Settings

Every setting is an `APP_*` environment variable declared once in [`internal/config`](internal/config/config.go) with a default, bounds and a doc line.

| Variable | Default | Meaning |
|---|---|---|
| `APP_DATABASE_DSN` | required | MySQL DSN |
| `APP_VALKEY_ADDR` | `localhost:6379` | Valkey address |
| `APP_PROVIDER_URL_SMS`, `_EMAIL`, `_PUSH` | `http://localhost:8090/send` | Provider URL per channel |
| `APP_PROVIDER_TIMEOUT` | `5s` | One provider call |
| `APP_MAX_ATTEMPTS` | `5` | Provider calls before a notification is `exhausted` |
| `APP_RETRY_BASE`, `APP_RETRY_CAP` | `1s`, `5m` | Backoff base and cap |
| `APP_RATE_LIMIT_PER_SECOND` | `100` | The provider's limit per channel |
| `APP_RATE_LIMIT_HEADROOM_PERCENT` | `3` | How far under the limit the limiter paces |
| `APP_RATE_LIMIT_HORIZON` | `200ms` | How far ahead send slots are reserved |
| `APP_PRIORITY_WEIGHTS` | `70,20,10` | Shares of high, normal, low |
| `APP_LEASE_DURATION`, `APP_REAP_INTERVAL` | `30s`, `5s` | Claim lease; how often expired leases are returned |
| `APP_WORKER_CONCURRENCY` | `32` | Provider calls in flight per worker |
| `APP_BREAKER_WINDOW`, `_MIN_REQUESTS`, `_FAILURE_RATIO`, `_COOLDOWN` | `10s`, `10`, `0.5`, `5s` | Circuit breaker |
| `APP_IDEMPOTENCY_RETENTION`, `APP_IDEMPOTENCY_SWEEP_INTERVAL` | `24h`, `1m` | How long a key is remembered; how often expired keys are deleted |
| `APP_WS_SEND_BUFFER` | `256` | Events buffered per WebSocket subscriber before it is dropped |
| `APP_OTLP_ENDPOINT` | empty | OTLP/HTTP endpoint; empty disables export |

To send to a real endpoint instead of the stand-in, export `PROVIDER_URL_SMS`, `PROVIDER_URL_EMAIL` or `PROVIDER_URL_PUSH` before `docker compose up`. A free webhook.site URL accepts about 100 requests in total, which this system sends in one second, so the stand-in is the default.

## Test

```bash
go tool -modfile=tools.mod task test:all
```

Unit tests, then the acceptance tests in [`test/acceptance`](test/acceptance): they build the binary, run `serve` and several `worker` processes against real MySQL and Valkey (containers) and the provider stand-in, and assert on what an API client and the provider observe. Each test names the requirement it proves; the map is [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md). Needs Go and Docker.

`go tool -modfile=tools.mod task check` is the full gate (build, vet, lint, tests, vulnerabilities); CI runs the same on every pull request.

## Where things are

| Path | What |
|---|---|
| `api/openapi.yaml` | HTTP contract; the server interface is generated from it and requests are validated against it at runtime |
| `schema/*.sql` | Desired database state; `migrations/` is generated from it by Atlas (versioned, goose format) |
| `internal/core` | Vocabulary, the state machine, the limits, and the ports between the parts |
| `internal/intake`, `internal/api` | Accepting, validating, reading, cancelling; HTTP handlers, WebSocket |
| `internal/worker`, `internal/limiter`, `internal/breaker`, `internal/provider` | Delivery loop, rate limit, circuit breaker, provider client and retry classifier |
| `internal/store` | SQL queries (sqlc), the queue, embedded migrations |
| `internal/realtime` | Status events over Valkey |
| `internal/obs`, `internal/config` | Logs, traces, metrics; every setting |
| `internal/fakeprovider`, `cmd/fakeprovider` | The provider stand-in: faults, dedupe switch, exact counts |
| `docs/decisions` | Why things are the way they are, one record per decision |
| `docs/AI-WORKFLOW.md`, `.agents/` | How AI agents built this: roles, briefs, reviews, mistakes and how each was caught |

## Develop

Only Go and Docker are needed; every tool is pinned in `tools.mod`.

```bash
go tool -modfile=tools.mod task --list
```
