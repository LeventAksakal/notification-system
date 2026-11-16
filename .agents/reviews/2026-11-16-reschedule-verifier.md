# Verifier review: reschedule and one clock (16 Nov 2026)

Fresh-context verifier on `03bbb6b..fc6d617`. Verdict: pass, nothing blocks. It confirmed the single conditional UPDATE, the race against the claim (the claim's row lock makes a waiting reschedule see claim_version 1), the changed-rows fallback, and the CHECK against every writer in queue.sql and intake.sql.

| # | Finding | Answer |
|---|---|---|
| 1 | Medium: the spec says `scheduled` can be rescheduled, but a row the breaker handed back shows as `scheduled` and is refused | Fixed: the operation description names that case; 0016 says so and leaves "scheduled requires claim_version 0" (which changes list and batch counts) to the owner |
| 2 | Medium: no acceptance tests for B2.1 and B2.2 in the repo | Held by the guard (owner item 0002) in `.agents/pr/reschedule-acceptance.md` |
| 3 | Low: ADD CONSTRAINT checks existing rows and copies the table; a row from the old process-clock check can fail it | Fixed in 0016: the pre-check query and the operational step |
| 4 | Low: "a row that passed once keeps passing" holds only while the clock does not step back | Fixed: comment and 0016 qualified; the step-back case is accepted as a clock fault |
| 5 | Low: the changed==0 success branch was exercised only within one millisecond | Fixed: the UPDATE no longer sets updated_at (ON UPDATE stamps it when the row changes), so the same future schedule twice changes 0 rows every time; a store test proves the CHECK measures from ON UPDATE's NOW(3) |
| 6 | Low: the "process clock ahead" case passes even if only the CHECK refused | Fixed: the batch refusal must carry an item index, which only the service's check gives |
| 7 | Low: Reschedule does not retry a deadlock or lock-wait timeout | Not changed: a single-row UPDATE by primary key, like Cancel; a lock wait longer than the request timeout ends as 503 or 500 either way |

The verifier could not run `go generate` (guard); the coordinator's regeneration after the fixes left the tree clean apart from the intended changes.
