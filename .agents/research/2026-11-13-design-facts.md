# Design facts checked on 13 Nov 2026

Web research by the researcher agent. V = the fetched page said it; S = search snippet only. Each item feeds a decision record.

## webhook.site as the provider (0006)
- A free URL accepts 100 requests (docs FAQ) or 50 (pricing page) and expires after 7 days; a 429 on a free URL usually means the cap was hit. V https://docs.webhook.site/ , https://webhook.site/pricing
- Status code and body of the default response are set when the URL is created (`default_status`, `default_content`, `default_content_type`) or in the UI; no paid plan needed. V https://docs.webhook.site/api/tokens.html
- Variables such as `$request.uuid$` and `$request.timestamp$` exist; whether they work in the default response body is not documented. Untested.
- Received requests can be searched by header: `GET /token/{id}/requests?query=headers.x-idempotency-key:"abc"`, limited to 120 calls per minute. V https://docs.webhook.site/api/requests.html
- Consequence: at 100 messages per second a free URL is exhausted in one second, so the compose default must be a local provider double.

## WebSocket libraries (0013)
- `github.com/coder/websocket` v1.8.15, 15 Jun 2026; context-based API. V https://pkg.go.dev/github.com/coder/websocket?tab=versions
- `gorilla/websocket` v1.5.3, 14 Jun 2024; not archived, no release for 17 months. V https://pkg.go.dev/github.com/gorilla/websocket?tab=versions
- The standard library has no WebSocket package.

## Shared rate limit of 100 per second (0009)
- GCRA: one stored value per key, O(1). With burst B, up to B+1 land at once; "never more than 100 in any second" needs burst 1, one message per 10 ms. Lua reference: https://raw.githubusercontent.com/go-redis/redis_rate/v10/lua.go (V; uses server `TIME`).
- Token bucket with capacity C: about C+100 can land in one second.
- Sliding-window log (sorted set of timestamps): exact, allows a burst of up to 100, O(log N), about 100 entries per key.
- `valkey-go`: `valkey.NewLuaScript(src).Exec(ctx, client, keys, args)` sends EVALSHA and falls back to EVAL. V https://github.com/valkey-io/valkey-go

## MySQL 8.4 as the queue (0003)
- `FOR UPDATE SKIP LOCKED` is documented for "queue-like" tables. V https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html
- InnoDB locks every row it scans: `ORDER BY … LIMIT` without an index matching the order locks far more than LIMIT rows and deadlocks. V https://bugs.mysql.com/bug.php?id=115246
- REPEATABLE READ takes gap locks on scanned ranges; READ COMMITTED locks index records only. V https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html
- Index `(channel, status, priority, due_at)` serves `WHERE channel=? AND status=? AND due_at<=? ORDER BY priority, due_at` without a sort; `(channel, status, due_at, priority)` would sort. Inferred from https://dev.mysql.com/doc/refman/8.4/en/order-by-optimization.html ; confirm with EXPLAIN.

## Retries and idempotency (0007, 0012)
- `Retry-After` is an HTTP date or a number of seconds. V RFC 9110 §10.2.3
- Convention, no single authority: retry 408, 425, 429 and 5xx; treat other 4xx as permanent.
- Full jitter: `sleep = random(0, min(cap, base * 2^attempt))`. https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/ (formula from secondary sources; the page shows it as an image)
- `Idempotency-Key`: IETF draft-ietf-httpapi-idempotency-key-header-08 (22 Oct 2026), not yet an RFC. 409 while the first request is still in flight, 422 when the key is reused with a different payload. V https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/
