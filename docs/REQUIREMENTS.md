# Requirements ledger

## Where it stopped
2026-11-13: scaffold only. No requirement is built. Design decisions 0002–0019 are proposed and wait for the owner.

## Requirements
One row per line of the brief that asks for something, in the brief's words. Status: open, failing test written, passing, measured.

| Id | Requirement (brief's words) | Check (observable outcome) | Decision | Status | Evidence |
|---|---|---|---|---|---|
| R1 | Create notification requests with recipient, channel, content, and priority | POST returns an id; GET shows the same fields and a status | 0005, 0017 | open | |
| R2 | Support batch creation (up to 1000 notifications per request) | 1000 items accepted in one request; 1001 refused with a clear error | 0011 | open | |
| R3 | Query notification status by ID | GET by id returns status, attempts and last error | 0005 | open | |
| R4 | Query notification status by batch ID | GET by batch id returns counts per status and the items | 0011 | open | |
| R5 | Cancel pending notifications | Cancel before sending: never reaches the provider. Cancel after sending: refused | 0005 | open | |
| R6 | List notifications with filtering (status, channel, date range) and pagination | Each filter narrows the list; pages do not repeat or skip rows while new rows arrive | 0004 | open | |
| R7 | Process notifications asynchronously via queue workers | POST returns before delivery; workers deliver; killing one worker loses nothing | 0002, 0003, 0018 | open | |
| R8 | Implement rate limiting: maximum 100 messages per second per channel | With 3 workers and a backlog, the provider never sees more than 100 per channel in any second | 0009 | open | |
| R9 | Priority queue support (high, normal, low) | A high message queued behind a large low backlog is sent within a bounded time; low traffic still moves under sustained high traffic | 0010 | open | |
| R10 | Content validation (character limits, required fields) | Over-limit or missing fields are refused with the field named; the same limit holds in the API, the service and the database | 0017 | open | |
| R11 | Idempotency support to prevent duplicate sends | The same request sent twice creates one notification; one notification reaches the provider once under worker crashes | 0006, 0012, 0018 | open | |
| R12 | Delivery & Retry Logic (the brief leaves the design open) | Transient provider failures are retried with growing gaps and end as sent; permanent ones fail after one attempt; exhausted ones are kept and can be replayed | 0006, 0007 | open | |
| R13 | Real-time metrics endpoint (queue depth, success/failure rates, latency) | The endpoint's numbers match what a test just produced | 0014 | open | |
| R14 | Structured logging with correlation IDs | One id given at the API appears on every log line of that notification in the API and the worker, and reaches the provider | 0019 | open | |
| R15 | Health check endpoint | Healthy when dependencies answer; unready when the database or Valkey is down | 0019 | open | |
| R16 | Provider: POST `{ "to", "channel", "content" }`, expected 202 `{ "messageId", "status", "timestamp" }` | The provider double receives exactly that body; the returned `messageId` is stored | 0006 | open | |
| B1 | Bonus: Failure Handling | Provider outage: the channel pauses, no attempts are burned, sending resumes; dead letters are listed and replayable | 0007, 0008 | open | |
| B2 | Bonus: Scheduled Notifications, allow scheduling notifications for future delivery | A notification scheduled for T is not sent before T and is sent shortly after | 0016 | open | |
| B3 | Bonus: Template System, message templates with variable substitution | A template plus variables produces the rendered content; a missing variable is refused | 0015 | open | |
| B4 | Bonus: WebSocket Updates, real-time status updates via WebSocket | A subscriber sees each status change in order without polling | 0013 | open | |
| B5 | Bonus: Distributed Tracing | One trace links the API request, the queue wait and the provider call | 0019 | open | |
| B6 | Bonus: GitHub Actions CI/CD, automated testing and linting pipeline | The workflow is green on the default branch | none | passing | First run green on `39f2d5a`: https://github.com/LeventAksakal/notification-system/actions/runs/43471895619 |
| D1 | Source code: repository with clean commit history | Linear history, conventional messages, each commit builds | none | open | |
| D2 | README.md: setup instructions, architecture overview, API examples | A fresh clone followed step by step works | none | open | |
| D3 | Docker Compose: one-command setup (`docker-compose up`) | Fresh clone, one command, stack healthy with no other setup | 0002 | open | |
| D4 | API documentation: Swagger/OpenAPI specification | The spec is served and matches the handlers (generated) | 0004 | open | |
| D5 | Database migrations: versioned schema changes | Migrations apply from empty; schema and migrations cannot drift | 0004 | open | |
| D6 | Test suite: runnable with single command | One command runs unit and acceptance tests | none | open | |
