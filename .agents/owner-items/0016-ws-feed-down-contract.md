# 0016 The spec does not name /v1/ws's "feed down" refusal (raised by realtime-seq, 16 Nov 2026)

Question: should `api/openapi.yaml` give `/v1/ws` its own 503 ("live updates are unavailable", no `Retry-After`) and name the 1012 close code that ends open streams when the event feed is lost? Today its 503 refers to `ServiceUnavailable`, which describes only the database and promises `Retry-After`.

Options:
1. Add a dedicated `LiveUpdatesUnavailable` response for the `/v1/ws` 503, and list the close codes (1008 too slow, 1012 feed lost, 1001 shutdown) in the operation description. Clients get the exact contract, but it changes the spec while the code-first (Huma) move is in flight.
2. Leave the spec as it is and keep decision 0013 "As built" as the reference. No churn, but clients learn the codes from the decision record instead of the contract.

Assumption taken: option 2 for now; the spec got one sentence (503 while the feed is down). Do option 1 with the Huma move.

Related:
- The R15 row in docs/REQUIREMENTS.md still says "unready when the database or Valkey is down". The owner narrowed readiness to MySQL (decision 0013, 15 Nov 2026). The coordinator owns that row.
- The acceptance tests for the new rule are drafted but not written: the guard refuses writes from the worktree (owner item 0002). The drafts are in the side-session notes, file `realtime-seq-acceptance-drafts.md`. Until they are applied, TestR15HealthAndReadiness, TestR15ReadyzMySQLDown, TestR15ReadyzValkeyDown and the TestGapOutage* tests fail on this branch.

## Answer (coordinator, 16 Nov 2026)
Option 1. At the rebase, the Huma WebSocket operation gets a 503 response "live updates are unavailable". Close code 1012 (service restart) goes into the WebSocket contract, which the AsyncAPI document carries after this lane lands. The R15 row stays with the coordinator.
