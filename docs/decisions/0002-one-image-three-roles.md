---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when API and worker need different release cycles
---
# One image runs as api, worker or migrate; workers scale by replica (0002)

## Rule
`cmd/app` has the subcommands `serve`, `worker` and `migrate`; no package under `internal/` may assume it is the only running instance.

## Context
The brief asks for "queue workers", "high throughput" and "one-command setup (docker-compose up)". Intake and delivery have different load shapes: intake is bursty, delivery is capped at 100 per second per channel.

## Options
1. One process serves the API and runs the workers. Simplest to run; a delivery stall or a worker crash takes the API with it, and the two cannot scale apart.
2. One binary, role chosen by subcommand; compose starts `migrate` once, `api`, and `worker` with 3 replicas. One build and one image; roles scale and fail separately; every test of "two workers at once" is real from the first day.
3. Separate binaries or modules per service. Independent releases; more build and CI surface with nothing to gain at this size.

## Decision
Pending. Recommended: option 2. It costs one `switch` in `main` and makes concurrency between workers something the default stack exercises, not a claim.

## Confirmation
`docker-compose.yml` runs `worker` with `deploy.replicas: 3`. The fault test stops one worker container while a backlog drains and asserts every notification ends `sent` exactly once.
