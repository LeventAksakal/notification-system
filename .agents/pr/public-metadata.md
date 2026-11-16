# GitHub metadata for the public repository

Drafted 16 Nov 2026 for the coordinator, who applies it when the repository goes public.

## Description (311 characters, limit 350)

Notification service in Go: accepts SMS, email and push over HTTP, queues them in MySQL and delivers them at up to 100 messages per second per channel, with priorities, retries, a circuit breaker, scheduling, templates and live status over WebSocket. Valkey, OpenTelemetry, runs with one docker compose command.

## Topics (10)

`go`, `golang`, `notifications`, `mysql`, `valkey`, `opentelemetry`, `rate-limiting`, `job-queue`, `websocket`, `docker-compose`

Why these: the language twice (people search both), the domain, the two stores, the telemetry stack, and the three properties a reviewer filters on (rate limit, a queue, live status). `golang` and `go` are both widely used topic tags.

## Homepage

Leave it empty. Nothing is deployed, and the API reference at `/docs` is served by the local stack only. A homepage pointing back into the repository repeats the README.

## Apply (one line; runs in bash, PowerShell and cmd)

```bash
gh repo edit LeventAksakal/notification-system --description "Notification service in Go: accepts SMS, email and push over HTTP, queues them in MySQL and delivers them at up to 100 messages per second per channel, with priorities, retries, a circuit breaker, scheduling, templates and live status over WebSocket. Valkey, OpenTelemetry, runs with one docker compose command." --homepage "" --add-topic go,golang,notifications,mysql,valkey,opentelemetry,rate-limiting,job-queue,websocket,docker-compose
```

## Settings worth a look when it goes public (the owner's call; not applied)

- Wiki and Projects tabs: unused here; `--enable-wiki=false --enable-projects=false` hides empty tabs.
- Secret scanning and push protection are free for public repositories (Settings → Code security).
- GitHub Actions on standard runners are free for public repositories, so the "CI minutes are billed" rule in `CLAUDE.md` stops applying once the repository is public.
- `ci.yml`'s optional `claude-review` job reads `secrets.ANTHROPIC_API_KEY`; pull requests from forks get no secrets, so it skips itself there. Nothing to change.
