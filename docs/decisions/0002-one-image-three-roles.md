---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when API and worker need different release cycles
---
# One image runs as api, worker or migrate; workers scale by replica (0002)

## Question
How does the system run: one program, or separate programs for taking requests in and for delivering them?

## Rule
`cmd/app` has the subcommands `serve`, `worker` and `migrate`, and every container names its role (there is no default); no package under `internal/` may assume it is the only running instance.

## Context
The brief asks for "queue workers", "high throughput" and "one-command setup (docker-compose up)". Intake and delivery have different load shapes: intake is bursty, delivery is capped at 100 per second per channel.

## Options
1. One process serves the API and runs the workers. Simplest to run; a delivery stall or a worker crash takes the API with it, and the two cannot scale apart.
2. One binary, role chosen by subcommand; compose starts `migrate` once, `api`, and `worker` with 3 replicas. One build and one image; roles scale and fail separately; every test of "two workers at once" is real from the first day.
3. Separate binaries or modules per service. Independent releases; more build and CI surface with nothing to gain at this size.
4. One source tree, a binary per role cut with build tags (`//go:build worker`). Measured 15 Nov: the stripped binary is 38 MB, of which code only the API needs is 5.1 MB (4.8 MB of it the embedded Scalar docs bundle); everything else (MySQL, Valkey, OpenTelemetry, gRPC) is shared. A worker-only binary would save 13%, but three role images each carry the shared 33 MB, so a node running all roles pulls about 105 MB instead of 38 MB, and CI builds and tests three tag sets.

## Decision
Option 2, with the role always named: `CMD ["serve"]` was removed from the Dockerfile and `app` without a role exits with the usage line, so a container that forgets its role fails instead of silently becoming an API. Option 4 was weighed against the owner's wish to keep API code out of the worker and declined on the numbers above. The replica count (3) lives in `docker-compose.yml`, not in the application's settings: it is the platform's dial, and three is the smallest count that exercises claim races and survives losing a worker; throughput alone would need one (the default 72 calls in flight, `config/config.go`, cover 300 per second while the provider answers in under about 100 ms).

## Confirmation
`docker-compose.yml` runs `worker` with `deploy.replicas: 3`. `TestDeliveryR11OnceUnderTimeoutsAndCrashes` ("worker killed while its sends are held", `test/acceptance/delivery_crash_test.go`) kills one worker process while a backlog drains and asserts every notification ends `sent` exactly once; killing a worker container was checked by hand on the compose stack (requirement row D3). `TestARoleMustBeNamed` (`cmd/app`) asserts a missing or unknown role is refused before any setting is read.
