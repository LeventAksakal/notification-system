# realtime-seq: acceptance tests to write after landing

From the realtime-seq session (branch claude/feat-realtime-sequence-readiness-647281). tools/agentguard refused the test-writer in the session worktree (owner item 0002), so nothing under test/acceptance/ changed on the branch. A test-writer running from the main checkout should apply these after the merge. Until then the following acceptance tests FAIL on the branch, because /readyz no longer checks or reports Valkey:
- TestR15HealthAndReadiness (asserts checks["valkey"]=="up")
- TestR15ReadyzMySQLDown (asserts the valkey check stays "up")
- TestR15ReadyzValkeyDown (asserts 503 with Valkey down)
- every TestGapOutage* test that calls valkeyDown (it waits for /readyz != 200), and TestGapOutageWSClosedWhileFeedLost.

Build tag is `integration`: `go test -tags integration ./test/acceptance/ -run 'TestR15|TestGapOutage' -count=1`. The drafts below are not compiled. The helpers they name (poll, probe, consistently, within, timedDo, dialWS, readEvent, msg, apiError, apiNotification, stackBehindValkeyProxy, linkProxy.down/up, outageClosed) exist in test/acceptance today, according to the test-writer.

## 1. test/acceptance/observe_outage_test.go
- Imports: add "net/url" and "strings".
- Header comment (lines 23-28): the instance stays ready and the API keeps answering promptly (decision 0013 as changed on 15 Nov 2026: /readyz checks MySQL only). WebSocket streams end instead of going silent, new subscriptions are refused with 503 until the feed is back, and workers deliver nothing while the limiter cannot be consulted.
- Replace valkeyDown and add these helpers:

```go
// valkeyDown takes Valkey away and waits (bounded) until GET /v1/ws (no filter) is refused with 503 unavailable,
// so what follows runs while the app knows Valkey is down. /readyz is no signal: it checks MySQL only (decision 0013).
func valkeyDown(t *testing.T, s *stack, p *linkProxy, o outage) {
	t.Helper()
	p.down(o)
	waitFeedRefused(t, s, 20*time.Second, "after Valkey went "+string(o))
}

type wsAnswer struct {
	Status int    // 0 = no answer
	Body   []byte // first 1 KiB of a refusal
	Err    error
}

func (a wsAnswer) String() string {
	e := "accepted"
	if a.Err != nil {
		e = a.Err.Error()
	}
	return "HTTP " + strconv.Itoa(a.Status) + " " + string(a.Body) + " (" + e + ")"
}

func (a wsAnswer) refusedUnavailable() bool {
	var e apiError
	return a.Err != nil && a.Status == http.StatusServiceUnavailable && json.Unmarshal(a.Body, &e) == nil && e.Code == "unavailable"
}

// tryWS performs one GET /v1/ws handshake and closes the socket at once if accepted.
func (s *stack) tryWS(kv ...string) wsAnswer {
	q := url.Values{}
	for i := 0; i+1 < len(kv); i += 2 {
		q.Set(kv[i], kv[i+1])
	}
	u := "ws" + strings.TrimPrefix(s.URL, "http") + "/v1/ws"
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, res, err := websocket.Dial(ctx, u, nil) // on refusal coder/websocket leaves up to 1 KiB of body in res.Body
	a := wsAnswer{Err: err}
	if res != nil {
		a.Status = res.StatusCode
		if err != nil && res.Body != nil {
			a.Body, _ = io.ReadAll(res.Body)
		}
	}
	if c != nil {
		_ = c.CloseNow()
	}
	return a
}

func waitFeedRefused(t *testing.T, s *stack, timeout time.Duration, when string, kv ...string) wsAnswer {
	t.Helper()
	var last wsAnswer
	if !poll(timeout, func() bool { last = s.tryWS(kv...); return last.refusedUnavailable() }) {
		t.Fatalf("not within %s %s: GET /v1/ws %q want 503 with code unavailable, last %s", timeout, when, kv, last)
	}
	return last
}
```

- Line 268 comment: "(decision 0013: Valkey down degrades live updates only, it does not stall intake)".
- Line 307 comment: "(R15 and decision 0013: the instance stays ready; create, read and cancel keep working, only live updates are refused)".
- TestGapOutageWSClosedWhileFeedLost:
  - At the stream-end branch, require `errors.As(err, &ce) && ce.Code == websocket.StatusServiceRestart`; otherwise Fatalf "want close 1012 (decision 0013)".
  - Replace lines 381-391 (waitReadyz and the dialWS 5xx check) with `a := waitFeedRefused(t, s, 20*time.Second, "after Valkey went away"); t.Logf("GET /v1/ws while Valkey is down: %s", a)`.
  - Delete line 397 (waitReadyz 200).
  - Declare `var status int; var err error` before the recovery poll, raise the poll to 30 s, and change its message to "still refused 30s after Valkey came back".

## 2. test/acceptance/observe_health_test.go
- Delete TestR15ReadyzValkeyDown.
- TestR15HealthAndReadiness: change the comment to "(decision 0019; 0013 as changed 15 Nov 2026: /readyz checks MySQL only)" and assert only `h.Checks["mysql"]=="up"`.
- dependencyDown(t, peer, p, dep string): drop the `other` parameter and its check. TestR15ReadyzMySQLDown calls `dependencyDown(t, peer, p, "mysql")`.

## 3. test/acceptance/observe_unavailable_test.go (import github.com/coder/websocket)

```go
// R15: Health check endpoint. The row in docs/REQUIREMENTS.md (owned by the coordinator) still reads "unready when the
// database or Valkey is down"; decision 0013 as changed and accepted by the owner on 15 Nov 2026 narrows readiness to
// MySQL: with the API's Valkey subscription down, /readyz answers 200, create and read work, GET /v1/ws answers 503
// unavailable; when Valkey returns a new subscription upgrades and gets its snapshot.
func TestR15ReadyWithValkeyDownLiveUpdatesRefused(t *testing.T) {
	const limit = 5 * time.Second
	s, p := stackBehindValkeyProxy(t, 0)
	before := s.create(t, msg("sms", "before the valkey outage"))
	p.down(outageClosed)
	a := waitFeedRefused(t, s, 20*time.Second, "after Valkey went away", "notificationId", before.ID)
	t.Logf("GET /v1/ws with Valkey down: %s", a)
	consistently(t, 2*time.Second, func() bool {
		st, h, err := probe(s.URL + "/readyz")
		return err == nil && st == http.StatusOK && h.Status == "ok" && h.Checks["mysql"] == "up"
	}, "/readyz must stay 200 ok (mysql up) while only Valkey is down")
	r, took := s.timedDo(t, http.MethodPost, "/v1/notifications", msg("email", "during the valkey outage"), 30*time.Second)
	within(t, "POST /v1/notifications", r, took, http.StatusCreated, limit)
	var n apiNotification
	r.decode(t, &n)
	r, took = s.timedDo(t, http.MethodGet, "/v1/notifications/"+n.ID, nil, 30*time.Second)
	within(t, "GET /v1/notifications/{id}", r, took, http.StatusOK, limit)
	if !p.up(t) {
		t.Skipf("could not listen on %s again to bring Valkey back; recovery step skipped", p.Addr)
	}
	var sub *websocket.Conn
	var status int
	var err error
	if !poll(30*time.Second, func() bool { sub, status, err = s.dialWS("notificationId", n.ID); return err == nil }) {
		t.Fatalf("GET /v1/ws?notificationId=%s still refused 30s after Valkey came back (HTTP %d): %v", n.ID, status, err)
	}
	t.Cleanup(func() { _ = sub.CloseNow() })
	first, err := readEvent(sub, time.Now().Add(10*time.Second))
	if err != nil || !first.Snapshot || first.NotificationID != n.ID || first.Status != "pending" {
		t.Fatalf("first message after Valkey came back: want snapshot of %s pending, got %v (%v)", n.ID, first, err)
	}
}
```
