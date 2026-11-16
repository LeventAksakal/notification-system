//go:build integration

package acceptance

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// Valkey outage: the instance stays ready and the API keeps answering
// promptly (decision 0013 as changed on 15 Nov 2026: /readyz checks MySQL
// only). WebSocket streams end instead of going silent, new subscriptions are
// refused with 503 until the feed is back, and workers deliver nothing while
// the limiter cannot be consulted. The app reaches Valkey only through linkProxy, a TCP
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
		last, _, _ = s.readyz()
		return ok(last)
	}) {
		t.Fatalf("not within %s: /readyz %s (last status %d, 0 = no answer)", timeout, what, last)
	}
}

// valkeyDown takes Valkey away and waits (bounded) until GET /v1/ws (no
// filter) is refused with 503 unavailable, so what follows runs while the app
// knows Valkey is down. /readyz is no signal: it checks MySQL only (decision 0013).
func valkeyDown(t *testing.T, s *stack, p *linkProxy, o outage) {
	t.Helper()
	p.down(o)
	waitFeedRefused(t, s, 20*time.Second, "after Valkey went "+string(o))
}

// wsAnswer is the outcome of one GET /v1/ws handshake.
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

// refusedUnavailable reports a 503 with the Error body and code unavailable
// (LiveUpdatesUnavailable in the contract).
func (a wsAnswer) refusedUnavailable() bool {
	var e apiclient.Error
	return a.Err != nil && a.Status == http.StatusServiceUnavailable && json.Unmarshal(a.Body, &e) == nil && e.Code == "unavailable"
}

// tryWS performs one GET /v1/ws handshake (query name/value pairs) and closes
// the socket at once if accepted. The generated client cannot speak
// WebSocket, so the handshake is dialled directly, as dialWS does.
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
			_ = res.Body.Close()
		}
	}
	if c != nil {
		_ = c.CloseNow()
	}
	return a
}

// waitFeedRefused polls GET /v1/ws until it is refused with 503 unavailable.
func waitFeedRefused(t *testing.T, s *stack, timeout time.Duration, when string, kv ...string) wsAnswer {
	t.Helper()
	var last wsAnswer
	if !poll(timeout, func() bool { last = s.tryWS(kv...); return last.refusedUnavailable() }) {
		t.Fatalf("not within %s %s: GET /v1/ws %q want 503 with code unavailable, last %s", timeout, when, kv, last)
	}
	return last
}

// apiCall is one typed request (a method of the generated client without
// "WithResponse"); the caller reads its raw answer.
type apiCall func(ctx context.Context) (*http.Response, error)

// timedDo sends one typed request with a hard client timeout and returns the
// response and how long it took. A timeout fails the test with a message that
// says the API did not answer in time while Valkey was down.
func (s *stack) timedDo(t *testing.T, what string, call apiCall, clientTimeout time.Duration) (*response, time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	start := time.Now()
	res, err := call(ctx)
	if err != nil {
		if ctx.Err() != nil {
			t.Fatalf("%s with Valkey down: no response within the client timeout of %s (publishing must not hold the response)", what, clientTimeout)
		}
		t.Fatalf("%s with Valkey down: %v", what, err)
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		if ctx.Err() != nil {
			t.Fatalf("%s with Valkey down: body not complete within the client timeout of %s", what, clientTimeout)
		}
		t.Fatalf("%s: read body: %v", what, err)
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
// (decision 0013: Valkey down degrades live updates only, it does not stall
// intake). CONTRACT
// "Events": "Publishing never delays a response by more than one bounded wait
// per request, whatever the number of events (a batch is one pipelined
// publish with one deadline)."
func TestGapOutageBatchAcceptedPromptlyWithValkeyDown(t *testing.T) {
	for _, o := range outages {
		t.Run("valkey "+string(o), func(t *testing.T) {
			s, p := stackBehindValkeyProxy(t, 0)
			valkeyDown(t, s, p, o)

			channels := []string{"sms", "email", "push"}
			items := make([]apiclient.NotificationCreate, 1000)
			for i := range items {
				items[i] = msg(channels[i%3], "outage batch "+strconv.Itoa(i))
			}
			r, took := s.timedDo(t, "POST /v1/notifications/batch", func(ctx context.Context) (*http.Response, error) {
				return s.API.CreateBatch(ctx, nil, apiclient.BatchCreate{Notifications: items})
			}, 30*time.Second)
			within(t, "POST /v1/notifications/batch (1000 items)", r, took, http.StatusCreated, 10*time.Second)
			var b apiclient.BatchCreated
			r.decode(t, &b)
			if b.Total != 1000 || len(b.NotificationIds) != 1000 {
				t.Fatalf("batch: want total 1000 and 1000 ids, got total %d and %d ids", b.Total, len(b.NotificationIds))
			}
			if got := s.count(t, "SELECT COUNT(*) FROM notifications WHERE batch_id = ?", b.Id.String()); got != 1000 {
				t.Fatalf("rows of batch %s: want 1000, got %d", b.Id, got)
			}

			r, took = s.timedDo(t, "POST /v1/notifications", func(ctx context.Context) (*http.Response, error) {
				return s.API.CreateNotification(ctx, nil, msg("sms", "outage single"))
			}, 30*time.Second)
			within(t, "POST /v1/notifications", r, took, http.StatusCreated, 5*time.Second)
			var n apiclient.Notification
			r.decode(t, &n)
			if got := s.count(t, "SELECT COUNT(*) FROM notifications WHERE id = ?", n.Id.String()); got != 1 {
				t.Fatalf("row of %s: want 1, got %d", n.Id, got)
			}
		})
	}
}

// R1: Create notification requests with recipient, channel, content, and
// priority - "POST returns an id; GET shows the same fields and a status",
// while Valkey is down (R15 and decision 0013: the instance stays ready;
// create, read and cancel keep working, only live updates are refused).
func TestGapOutageAPIServesWithValkeyDown(t *testing.T) {
	const limit = 5 * time.Second
	const clientTimeout = 30 * time.Second
	for _, o := range outages {
		t.Run("valkey "+string(o), func(t *testing.T) {
			s, p := stackBehindValkeyProxy(t, 0)
			valkeyDown(t, s, p, o)

			r, took := s.timedDo(t, "POST /v1/notifications", func(ctx context.Context) (*http.Response, error) {
				return s.API.CreateNotification(ctx, nil, msg("email", "outage r1"))
			}, clientTimeout)
			within(t, "POST /v1/notifications", r, took, http.StatusCreated, limit)
			var n apiclient.Notification
			r.decode(t, &n)

			r, took = s.timedDo(t, "GET /v1/notifications/{id}", func(ctx context.Context) (*http.Response, error) {
				return s.API.GetNotification(ctx, n.Id)
			}, clientTimeout)
			within(t, "GET /v1/notifications/{id}", r, took, http.StatusOK, limit)
			var got apiclient.Notification
			r.decode(t, &got)
			if got.Id != n.Id || got.Status != "pending" || got.Channel != "email" || got.To != emailTo {
				t.Errorf("GET %s: want the pending email just created, got %s", n.Id, jsonOf(got))
			}

			r, took = s.timedDo(t, "GET /v1/notifications", func(ctx context.Context) (*http.Response, error) {
				return s.API.ListNotifications(ctx, &apiclient.ListNotificationsParams{Channel: ptr(apiclient.Email)})
			}, clientTimeout)
			within(t, "GET /v1/notifications", r, took, http.StatusOK, limit)
			var page apiclient.NotificationPage
			r.decode(t, &page)
			found := false
			for _, it := range page.Items {
				found = found || it.Id == n.Id
			}
			if !found {
				t.Errorf("list: want %s among the items, got %v", n.Id, ids(page.Items))
			}

			r, took = s.timedDo(t, "POST /v1/notifications/{id}/cancel", func(ctx context.Context) (*http.Response, error) {
				return s.API.CancelNotification(ctx, n.Id)
			}, clientTimeout)
			within(t, "POST /v1/notifications/{id}/cancel", r, took, http.StatusOK, limit)
			if st := s.storedStatus(t, n.Id); st != "cancelled" {
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
		if errors.Is(err, errEventIncomplete) {
			t.Errorf("event before the stream ended: %v", err)
			continue
		}
		if err != nil {
			var ce websocket.CloseError
			if !errors.As(err, &ce) || ce.Code != websocket.StatusServiceRestart {
				t.Fatalf("stream ended after %s with %v: want close 1012 service restart (decision 0013)", time.Since(closedAt).Round(time.Millisecond), err)
			}
			t.Logf("stream ended after %s with close %d %q", time.Since(closedAt).Round(time.Millisecond), ce.Code, ce.Reason)
			break
		}
		t.Logf("event before the stream ended: %s", eventString(e))
	}

	// While the feed is lost a new handshake is refused with 503 unavailable.
	a := waitFeedRefused(t, s, 20*time.Second, "after Valkey went away")
	t.Logf("GET /v1/ws while Valkey is down: %s", a)

	// Valkey back: a new subscription works again, snapshot then live events.
	if !p.up(t) {
		t.Skipf("could not listen on %s again to bring Valkey back; recovery step skipped", p.Addr)
	}

	n := s.create(t, msg("push", "outage after recovery"))
	var sub *websocket.Conn
	var status int
	var err error
	if !poll(30*time.Second, func() bool {
		sub, status, err = s.dialWS("notificationId", n.Id.String())
		return err == nil
	}) {
		t.Fatalf("GET /v1/ws?notificationId=%s still refused 30s after Valkey came back (HTTP %d): %v", n.Id, status, err)
	}
	t.Cleanup(func() { _ = sub.CloseNow() })

	first, err := readEvent(sub, time.Now().Add(10*time.Second))
	if err != nil {
		t.Fatalf("first message after recovery: %v", err)
	}
	if !isSnapshot(first) || first.NotificationId != n.Id || first.Status != "pending" {
		t.Fatalf("first message after recovery: want snapshot of %s pending, got %s", n.Id, eventString(first))
	}
	s.cancel(t, n.Id)
	collectUntil(t, sub, 10*time.Second, reached(n.Id, "cancelled"), "live cancelled event of "+n.Id.String()+" after recovery")
}

// R7: Process notifications asynchronously via queue workers, and R8:
// "maximum 100 messages per second per channel". The limiter lives in Valkey;
// without it a worker must not send (fail closed) and must not burn attempts
// or fail notifications.
func TestGapOutageWorkerSendsNothingWithValkeyDown(t *testing.T) {
	s, p := stackBehindValkeyProxy(t, 1)

	// Baseline: through this proxy the worker does deliver.
	base := s.create(t, msg("sms", "outage baseline"))
	s.providerRequests(t, base.Id, 20*time.Second)
	s.waitStatus(t, base.Id, "sent", 20*time.Second)
	before := providerReceived(s)

	valkeyDown(t, s, p, outageClosed)

	created := make([]string, 0, 20)
	channels := []string{"sms", "email", "push"}
	for i := range 20 {
		created = append(created, s.create(t, msg(channels[i%3], "outage worker "+strconv.Itoa(i))).Id.String())
	}

	// consistently for 3 s, with the observed numbers in the failure message.
	q := "SELECT COUNT(*) FROM notifications WHERE id <> ? AND (status <> 'pending' OR attempts <> 0)"
	deadline := time.Now().Add(3 * time.Second)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		extra := providerReceived(s) - before
		moved := s.count(t, q, base.Id.String())
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
	if got := s.count(t, "SELECT COUNT(*) FROM delivery_attempts WHERE notification_id <> ?", base.Id.String()); got != 0 {
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
