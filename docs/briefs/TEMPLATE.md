# Brief: <slice name>

Fields chosen by evidence: stating the requirements and the interface roughly triples resolve rates (SWE-Bench Pro, https://arxiv.org/abs/2509.16941); a runnable "done when" beats prose.

- **Goal:** one sentence, the user-visible outcome.
- **Target behaviour (first):** what is observably true when done, e.g. "POST /v1/x twice with the same key returns the first response; one row exists".
- **Requirements → tests:** row ids from `docs/REQUIREMENTS.md`, each with its acceptance test path.
- **Contract (frozen):** the spec paths, schema tables and interfaces you build against. Do not change them; raise an owner item.
- **Owns / reads:** paths you may write; paths you should read first (with line ranges).
- **Constraints and non-goals:** what not to do or touch.
- **Done when:** the exact commands that must pass, e.g. `go test ./test/acceptance -run TestSendOnce` and `go vet ./internal/sender`.
- **Scope signal:** expected size (files, ~lines) so an overrun is visible.
- **Stop conditions:** when to stop and hand back instead of continuing (never to ask).
- **Hand-back (≤ 40 lines):** what passes, what does not, owner items raised, files touched.
