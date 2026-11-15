---
status: accepted
date: 2026-11-15
decision-makers: owner (in chat)
revisit: when a second deployable or a library consumer appears
---
# Packages are named for their role; contracts and settings sit at the root (0022)

## Rule
Contracts and settings are top-level folders (`api/`, `schema/`, `config/`); Go code lives under `internal/` in packages named for what they do: domain (`notification`), use cases (`intake`, `delivery` with `limiter/` and `breaker/` under it), transport adapters (`httpapi`, which also serves the WebSocket), outbound adapters (`store`, `provider`, `realtime`), and `telemetry`; `cmd/app` only wires them. No type is written by hand where a contract generates it.

## Context
Owner, 15 Nov 2026: "internal seems cryptic for the source code ... rather than hiding the configuration under source code I argue it should be top level"; then "you may keep internal if its the standard go convention but reorganize the other stuff so its idiomatic and an outsider has a convenient time exploring"; "websocket message scheme is similar to openapi spec ... those are network contracts". The layout had `internal/core`, `internal/obs`, `internal/api` (beside the top-level `api/` contract), `internal/config`, and the delivery parts (worker, limiter, breaker) side by side with everything else.

## Options
1. Drop `internal/`, domain packages at the root (as Prometheus does). Readable paths; about ten more root folders; `internal/` protects nothing in an app module but is what Go's layout guide recommends for servers.
2. Keep `internal/`, rename and group by role, settings contract to the root. Idiomatic, readable names, one move.
3. Keep the layout. No churn; `core`, `obs` and the scattered delivery parts stay cryptic.

## Decision
Option 2, the owner's choice. `core` became `notification` ("core" names nothing), `api` became `httpapi` (no clash with the `api/` contract), `worker` became `delivery` with its policies nested, `obs` became `telemetry`, `internal/config` became `config/`. The WebSocket stays in `httpapi`: it is one GET route on the same server, and a package of its own would need a second server type to share the mux. Its message type, `StatusEvent`, is generated from `api/openapi.yaml` (`skip-prune`), replacing a hand-written copy. AsyncAPI was considered for the WebSocket contract and not adopted: with one server-to-client message it would repeat the route and its parameters already in the OpenAPI document; it earns its place when clients send messages or message types multiply.

## Confirmation
`go build ./...`; `.claude/agent-policy.json` lists the contracts (`api/**`, `schema/**`, `internal/notification/**`) and `tools/ownercheck` enforces them at merge; `grep -rn 'type statusEvent' internal/` finds nothing.
