| `internal/fakeprovider`, `cmd/fakeprovider`, `test/support` | Provider double (faults, dedupe, delivery counts; compose service `provider`) and test helpers for MySQL, Valkey and the double |
# Notification system

An event-driven notification service. The notification features are not built yet: today the repo holds the service skeleton (config, telemetry, OpenAPI-first HTTP, sqlc, migrations), the compose stack and CI, with one example endpoint.

## Run it (needs only Docker)

```bash
docker compose up --build --wait
```

- API: http://localhost:8080/readyz, http://localhost:8080/v1/meta/{key}
- Grafana (traces, metrics, logs): http://localhost:3000

`docker compose down` stops it; add `-v` to drop the database volume.

## Where things are

| Path | What |
|---|---|
| `api/openapi.yaml` | HTTP contract; handlers are generated from it, and requests are validated against it at runtime |
| `schema/*.sql` | Desired database state; migrations in `migrations/` are generated from it by Atlas |
| `internal/config` | Every setting (`APP_*` env vars) with its default, bounds and doc |
| `internal/api` | Handlers |
| `internal/store` | SQL queries (sqlc), connection, embedded migrations |
| `internal/obs` | Logs, traces and metrics to OpenTelemetry |
| `cmd/app` | `serve`, `migrate`, `healthcheck` |
| `docs/decisions` | Why things are the way they are (ADRs) |
| `.agents/logs` | What the agents did, pivot by pivot |

## Develop

Only Go is needed; every tool is pinned in `tools.mod`.

```bash
go tool -modfile=tools.mod task --list
```
