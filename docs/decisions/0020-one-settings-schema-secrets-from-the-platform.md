---
status: accepted
date: 2026-11-14
decision-makers: owner (in chat)
revisit: when a second service or a shared development secret (a real provider key) appears
---
# One settings schema in code; secret values come from the platform, never from the repository (0020)

## Rule
Every setting and secret of the service is declared once in `config` (name, type, default, bounds, doc, secret or not). Every environment (local, test, stage, production) supplies values against that one shape; the binary refuses to start, and `app env check` refuses to pass, when the environment does not fit. A secret has no default and is supplied at run time by whatever manages secrets where the service runs, as an environment variable or as a mounted file (`<NAME>_FILE`). No secret value is committed, encrypted or not.

## Context
The repository had a hand-kept `.env.example` beside the schema in code: two shapes that drift. The owner asked for one shape that every environment abides by and an idiomatic way to manage secrets (14 Nov 2026).

## Options
1. Schema in code, values from the platform's secrets manager (chosen). Values may differ per environment; the shape may not. Nothing to decrypt, nothing to distribute.
2. Encrypted values in the repository (sops with age keys): a newcomer decrypts and runs. Considered and dropped by the owner the same day: larger organisations manage deployed secrets in a dedicated system, not in the repository, and the local stack's only secrets are throwaway passwords of containers that live on the developer's machine, so encryption would add a key to hand out and protect nothing.
3. A committed `.env` per environment: secrets in git.

## Decision
Option 1. The local compose stack keeps working with no setup: its MySQL passwords are development-only fallbacks in `docker-compose.yml`, overridable from the environment. Revisit option 2 when local development needs a secret that is real (a provider sandbox key shared by the team).

## Confirmation
`go test ./config`: every setting has a default or is required, bounds and a doc line; a secret is read from `<NAME>` or `<NAME>_FILE`, never both; `app env check` reports an unknown `APP_` variable and never prints a secret's value. `TestEnvExampleListsEverySettingOnceWithItsDefault` (`config/env_test.go`) and `TestEveryDialIsDocumentedAndHasADefaultOrIsInjected`: `.env.example` and the settings agree, and every dial has its doc line.
