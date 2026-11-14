//go:build integration

package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

// Valkey outage: the API keeps answering promptly, WebSocket streams end
// instead of going silent, and workers deliver nothing while the limiter
// cannot be consulted. The app reaches Valkey only through linkProxy, a TCP
// proxy the test can close (the port refuses connections, as in the R15
// readiness tests), turn unresponsive (connections are accepted and nothing
// ever answers: a hung or partitioned Valkey), or bring back on the same port.

type outage string

const (
	outageClosed       outage = "closed"       // the port refuses connections
	outageUnresponsive outage = "unresponsive" // connections hang, no reply
)

// linkProxy forwards a fixed local port to target and can take the link down
// and back up without changing the port.
type linkProxy struct {
	Addr   string
	target string

	mu    sync.Mutex
	ln    net.Listener // nil while closed
	hang  bool
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

func newLinkProxy(t *testing.T, target string) *linkProxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &linkProxy{Addr: ln.Addr().String(), target: target, ln: ln, conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go p.accept(ln)
	t.Cleanup(func() {
		p.down(outageClosed)
		p.wg.Wait()
	})
	return p
}

func (p *linkProxy) accept(ln net.Listener) {
	defer p.wg.Done()
	for {
		in, err := ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		hang := p.hang
		p.mu.Unlock()
		if hang {
			if p.track(ln, in) {
				p.wg.Add(1)
				go p.sink(in)
			}
			continue
		}
		out, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			_ = in.Close()
			continue
		}
		if !p.track(ln, in, out) {
			continue
		}
		p.wg.Add(2)
		go p.pipe(out, in)
		go p.pipe(in, out)
	}
}

// track registers conns unless ln is no longer the live listener.
func (p *linkProxy) track(ln net.Listener, cs ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != ln {
		for _, c := range cs {
			_ = c.Close()
		}
		return false
	}
	for _, c := range cs {
		p.conns[c] = struct{}{}
	}
	return true
}

func (p *linkProxy) untrack(cs ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range cs {
		delete(p.conns, c)
	}
}

func (p *linkProxy) pipe(dst, src net.Conn) {
	defer p.wg.Done()
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
	p.untrack(dst, src)
}

// sink reads and drops everything: the peer never gets a reply.
func (p *linkProxy) sink(c net.Conn) {
	defer p.wg.Done()
	_, _ = io.Copy(io.Discard, c)
	_ = c.Close()
	p.untrack(c)
}

// down drops every open connection and then either closes the port or keeps
// accepting connections that never get an answer.
func (p *linkProxy) down(o outage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if o == outageClosed {
		if p.ln != nil {
			_ = p.ln.Close()
			p.ln = nil
		}
	} else {
		p.hang = true
	}
	for c := range p.conns {
		_ = c.Close()
	}
}

// up forwards again on the same port; ok is false when the port could not be
// bound again within 10 s.
func (p *linkProxy) up(t *testing.T) bool {
	t.Helper()
	p.mu.Lock()
	p.hang = false
	open := p.ln != nil
	p.mu.Unlock()
	if open {
		return true
	}
	var ln net.Listener
	if !poll(10*time.Second, func() bool {
		var err error
		ln, err = (&net.ListenConfig{}).Listen(t.Context(), "tcp", p.Addr)
		return err == nil
	}) {
		return false
	}
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	p.wg.Add(1)
	go p.accept(ln)
	return true
}

// stackBehindValkeyProxy starts a stack whose serve (and workers) reach Valkey
// only through a linkProxy.
func stackBehindValkeyProxy(t *testing.T, workers int) (*stack, *linkProxy) {
	t.Helper()
	p := newLinkProxy(t, shared(t).valkeyAddr)
	s := startStack(t, stackOptions{Workers: workers, Env: map[string]string{"APP_VALKEY_ADDR": p.Addr}})
	return s, p
}

// waitReadyz polls /readyz (5 s per probe) until ok accepts the status (0 when
// no answer arrived), failing after timeout with the last status seen.
func waitReadyz(t *testing.T, s *stack, timeout time.Duration, what string, ok func(int) bool) {
	t.Helper()
	last := -1
	if !poll(timeout, func() bool {
		last, _, _ = probe(s.URL + "/readyz")
		return ok(last)
	}) {
		t.Fatalf("not within %s: /readyz %s (last status %d, 0 = no answer)", timeout, what, last)
	}
}

// valkeyDown takes Valkey away and waits (bounded) until /readyz stops
// answering 200, so what follows runs while the app knows Valkey is down.
func valkeyDown(t *testing.T, s *stack, p *linkProxy, o outage) {
	t.Helper()
	p.down(o)
	waitReadyz(t, s, 20*time.Second, "kept answering 200 after Valkey went "+string(o), func(st int) bool { return st != http.StatusOK })
}

// timedDo sends one JSON request with a hard client timeout and returns the
// response and how long it took. A timeout fails the test with a message that
// says the API did not answer in time while Valkey was down.
func (s *stack) timedDo(t *testing.T, method, path string, body any, clientTimeout time.Duration) (*response, time.Duration) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encode body: %v", err)
		}
		rdr = bytes.NewReader(buf)
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, s.URL+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if rdr != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	start := time.Now()
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			t.Fatalf("%s %s with Valkey down: no response within the client timeout of %s (publishing must not hold the response)", method, path, clientTimeout)
		}
		t.Fatalf("%s %s with Valkey down: %v", method, path, err)
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		if ctx.Err() != nil {
			t.Fatalf("%s %s with Valkey down: body not complete within the client timeout of %s", method, path, clientTimeout)
		}
		t.Fatalf("%s %s: read body: %v", method, path, err)
	}
	return &response{Status: res.StatusCode, Header: res.Header, Body: b}, time.Since(start)
}

// within requires a response with status want that took at most limit.
func within(t *testing.T, what string, r *response, took time.Duration, want int, limit time.Duration) {
	t.Helper()
	if r.Status != want {
		t.Fatalf("%s with Valkey down: want %d, got %s (after %s)", what, want, r, took)
	}
	if took > limit {
		t.Fatalf("%s with Valkey down: answered %d after %s, want within %s", what, r.Status, took, limit)
	}
	t.Logf("%s with Valkey down: %d in %s", what, r.Status, took.Round(time.Millisecond))
}

var outages = []outage{outageClosed, outageUnresponsive}

// R2: Support batch creation (up to 1000 notifications per request), and R15
// (Valkey down makes the instance unready, it does not stall intake). CONTRACT
// "Events": "Publishing never delays a response by more than one bounded wait
// per request, whatever the number of events (a batch is one pipelined
// publish with one deadline)."
func TestGapOutageBatchAcceptedPromptlyWithValkeyDown(t *testing.T) {
	for _, o := range outages {
		t.Run("valkey "+string(o), func(t *testing.T) {
			s, p := stackBehindValkeyProxy(t, 0)
			valkeyDown(t, s, p, o)

			channels := []string{"sms", "email", "push"}
			items := make([]map[string]any, 1000)
			for i := range items {
				items[i] = msg(channels[i%3], "outage batch "+strconv.Itoa(i))
			}
			r, took := s.timedDo(t, http.MethodPost, "/v1/notifications/batch", map[string]any{"notifications": items}, 30*time.Second)
			within(t, "POST /v1/notifications/batch (1000 items)", r, took, http.StatusCreated, 10*time.Second)
			var b batchCreated
			r.decode(t, &b)
			if b.Total != 1000 || len(b.NotificationIDs) != 1000 {
				t.Fatalf("batch: want total 1000 and 1000 ids, got total %d and %d ids", b.Total, len(b.NotificationIDs))
			}
			if got := s.count(t, "SELECT COUNT(*) FROM notifications WHERE batch_id = ?", b.ID); got != 1000 {
				t.Fatalf("rows of batch %s: want 1000, got %d", b.ID, got)
			}

			r, took = s.timedDo(t, http.MethodPost, "/v1/notifications", msg("sms", "outage single"), 30*time.Second)
			within(t, "POST /v1/notifications", r, took, http.StatusCreated, 5*time.Second)
			var n notification
			r.decode(t, &n)
			if got := s.count(t, "SELECT COUNT(*) FROM notifications WHERE id = ?", n.ID); got != 1 {
				t.Fatalf("row of %s: want 1, got %d", n.ID, got)
			}
		})
	}
}

// R1: Create notification requests with recipient, channel, content, and
// priority - "POST returns an id; GET shows the same fields and a status",
// while Valkey is down (R15: Valkey down makes the instance unready, not
// unusable).
func TestGapOutageAPIServesWithValkeyDown(t *testing.T) {
	const limit = 5 * time.Second
	const clientTimeout = 30 * time.Second
	for _, o := range outages {
		t.Run("valkey "+string(o), func(t *testing.T) {
			s, p := stackBehindValkeyProxy(t, 0)
			valkeyDown(t, s, p, o)

			r, took := s.timedDo(t, http.MethodPost, "/v1/notifications", msg("email", "outage r1"), clientTimeout)
			within(t, "POST /v1/notifications", r, took, http.StatusCreated, limit)
			var n notification
			r.decode(t, &n)

			r, took = s.timedDo(t, http.MethodGet, "/v1/notifications/"+n.ID, nil, clientTimeout)
			within(t, "GET /v1/notifications/{id}", r, took, http.StatusOK, limit)
			var got notification
			r.decode(t, &got)
			if got.ID != n.ID || got.Status != "pending" || got.Channel != "email" || got.To != emailTo {
				t.Errorf("GET %s: want the pending email just created, got %+v", n.ID, got)
			}

			r, took = s.timedDo(t, http.MethodGet, "/v1/notifications?channel=email", nil, clientTimeout)
			within(t, "GET /v1/notifications", r, took, http.StatusOK, limit)
			var page notificationPage
			r.decode(t, &page)
			found := false
			for _, it := range page.Items {
				found = found || it.ID == n.ID
			}
			if !found {
				t.Errorf("list: want %s among the items, got %v", n.ID, ids(page.Items))
			}

			r, took = s.timedDo(t, http.MethodPost, "/v1/notifications/"+n.ID+"/cancel", nil, clientTimeout)
			within(t, "POST /v1/notifications/{id}/cancel", r, took, http.StatusOK, limit)
			if st := s.storedStatus(t, n.ID); st != "cancelled" {
				t.Errorf("after cancel: stored status want cancelled, got %q", st)
			}
		})
	}
}

// B4: Bonus: WebSocket Updates - "a subscriber sees each status change in
// order"; it must never see a stream with holes. CONTRACT "Events": "When an
// API instance loses its event feed it closes its WebSocket streams and
// refuses new ones until the feed is back." (api/openapi.yaml /v1/ws.)
func TestGapOutageWSClosedWhileFeedLost(t *testing.T) {
	s, p := stackBehindValkeyProxy(t, 0)
	c := s.subscribe(t)

	p.down(outageClosed)
	closedAt := time.Now()

	// The open stream ends within 10 s: a close frame or a read error both
	// count; staying open and silent does not.
	for {
		e, err := readEvent(c, closedAt.Add(10*time.Second))
		if errors.Is(err, errWSTimeout) {
			t.Fatalf("Valkey down for 10s: the subscriber's stream is still open and silent (want the server to close it)")
		}
		if err != nil {
			var ce websocket.CloseError
			if errors.As(err, &ce) {
				t.Logf("stream ended after %s with close %d %q", time.Since(closedAt).Round(time.Millisecond), ce.Code, ce.Reason)
			} else {
				t.Logf("stream ended after %s: %v", time.Since(closedAt).Round(time.Millisecond), err)
			}
			break
		}
		t.Logf("event before the stream ended: %v", e)
	}

	// While the feed is lost a new handshake is refused with a 5xx.
	waitReadyz(t, s, 20*time.Second, "kept answering 200 after Valkey went away", func(st int) bool { return st != http.StatusOK })
	nc, status, err := s.dialWS()
	if err == nil {
		_ = nc.CloseNow()
		t.Fatalf("GET /v1/ws while Valkey is down: handshake accepted (want it refused with 5xx)")
	}
	if status < 500 {
		t.Fatalf("GET /v1/ws while Valkey is down: want HTTP >= 500, got %d (%v)", status, err)
	}
	t.Logf("GET /v1/ws while Valkey is down: refused with HTTP %d", status)

	// Valkey back: a new subscription works again, snapshot then live events.
	if !p.up(t) {
		t.Skipf("could not listen on %s again to bring Valkey back; recovery step skipped", p.Addr)
	}
	waitReadyz(t, s, 30*time.Second, "never answered 200 after Valkey came back", func(st int) bool { return st == http.StatusOK })

	n := s.create(t, msg("push", "outage after recovery"))
	var sub *websocket.Conn
	if !poll(20*time.Second, func() bool {
		sub, status, err = s.dialWS("notificationId", n.ID)
		return err == nil
	}) {
		t.Fatalf("GET /v1/ws?notificationId=%s still refused 20s after /readyz was 200 again (HTTP %d): %v", n.ID, status, err)
	}
	t.Cleanup(func() { _ = sub.CloseNow() })

	first, err := readEvent(sub, time.Now().Add(10*time.Second))
	if err != nil {
		t.Fatalf("first message after recovery: %v", err)
	}
	if !first.Snapshot || first.NotificationID != n.ID || first.Status != "pending" {
		t.Fatalf("first message after recovery: want snapshot of %s pending, got %v", n.ID, first)
	}
	s.cancel(t, n.ID)
	collectUntil(t, sub, 10*time.Second, reached(n.ID, "cancelled"), "live cancelled event of "+n.ID+" after recovery")
}

// R7: Process notifications asynchronously via queue workers, and R8:
// "maximum 100 messages per second per channel". The limiter lives in Valkey;
// without it a worker must not send (fail closed) and must not burn attempts
// or fail notifications.
func TestGapOutageWorkerSendsNothingWithValkeyDown(t *testing.T) {
	s, p := stackBehindValkeyProxy(t, 1)

	// Baseline: through this proxy the worker does deliver.
	base := s.create(t, msg("sms", "outage baseline"))
	s.providerRequests(t, base.ID, 20*time.Second)
	s.waitStatus(t, base.ID, "sent", 20*time.Second)
	before := providerReceived(s)

	valkeyDown(t, s, p, outageClosed)

	created := make([]string, 0, 20)
	channels := []string{"sms", "email", "push"}
	for i := range 20 {
		created = append(created, s.create(t, msg(channels[i%3], "outage worker "+strconv.Itoa(i))).ID)
	}

	// consistently for 3 s, with the observed numbers in the failure message.
	q := "SELECT COUNT(*) FROM notifications WHERE id <> ? AND (status <> 'pending' OR attempts <> 0)"
	deadline := time.Now().Add(3 * time.Second)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		extra := providerReceived(s) - before
		moved := s.count(t, q, base.ID)
		if extra != 0 || moved != 0 {
			t.Fatalf("with Valkey down the worker went ahead: provider received %d new requests (want 0, the limiter fails closed); %d notifications not pending or with attempts (want 0)",
				extra, moved)
		}
		if time.Now().After(deadline) {
			break
		}
		<-tick.C
	}

	for _, id := range created {
		if rs := s.Provider.Requests(fakeprovider.Filter{Key: id}); len(rs) != 0 {
			t.Errorf("notification %s reached the provider while Valkey was down", id)
		}
	}
	if got := s.count(t, "SELECT COUNT(*) FROM delivery_attempts WHERE notification_id <> ?", base.ID); got != 0 {
		t.Errorf("delivery_attempts while Valkey was down: want 0, got %d", got)
	}
	if got := s.count(t, "SELECT COUNT(*) FROM notifications WHERE status = 'failed'"); got != 0 {
		t.Errorf("failed notifications while Valkey was down: want 0, got %d", got)
	}
	t.Logf("20 notifications stayed pending with 0 attempts for 3s with Valkey down; worker exited: %t", s.workers[0].exited())
}

// providerReceived is every request the provider double received, all channels.
func providerReceived(s *stack) int {
	n := 0
	for _, c := range s.Provider.Stats().Channels {
		n += c.Received
	}
	return n
}
