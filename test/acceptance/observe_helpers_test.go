//go:build integration

package acceptance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// ---- metrics ----

// The shapes are the generated ones: apiclient.Metrics, QueueDepth,
// DeliveryTotals, DeliveryStats, LatencyMs, Health and StatusEvent.

// metrics fetches GET /v1/metrics (windowSeconds 0: the parameter is left
// out) and requires 200.
func (s *stack) metrics(t *testing.T, windowSeconds int) apiclient.Metrics {
	t.Helper()
	p := apiclient.GetMetricsParams{}
	if windowSeconds != 0 {
		p.WindowSeconds = &windowSeconds
	}
	r, err := s.API.GetMetricsWithResponse(t.Context(), &p)
	if err != nil {
		t.Fatalf("GET /v1/metrics (windowSeconds %d): %v", windowSeconds, err)
	}
	if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
		t.Fatalf("GET /v1/metrics (windowSeconds %d): want 200, got %s", windowSeconds, show(r))
	}
	return *r.JSON200
}

// queueByChannel indexes m.Queue by channel and fails unless it holds exactly
// one entry for each of sms, email and push.
func queueByChannel(t *testing.T, m apiclient.Metrics) map[string]apiclient.QueueDepth {
	t.Helper()
	out := map[string]apiclient.QueueDepth{}
	for _, q := range m.Queue {
		if _, dup := out[string(q.Channel)]; dup {
			t.Fatalf("queue has two entries for %q: %+v", q.Channel, m.Queue)
		}
		out[string(q.Channel)] = q
	}
	for _, ch := range []string{"sms", "email", "push"} {
		if _, ok := out[ch]; !ok || len(out) != 3 {
			t.Fatalf("queue: want exactly one entry per channel (sms, email, push), got %+v", m.Queue)
		}
	}
	return out
}

func deliveryByChannel(t *testing.T, m apiclient.Metrics) map[string]apiclient.DeliveryStats {
	t.Helper()
	out := map[string]apiclient.DeliveryStats{}
	for _, d := range m.DeliveryByChannel {
		if _, dup := out[string(d.Channel)]; dup {
			t.Fatalf("deliveryByChannel has two entries for %q: %+v", d.Channel, m.DeliveryByChannel)
		}
		out[string(d.Channel)] = d
	}
	for _, ch := range []string{"sms", "email", "push"} {
		if _, ok := out[ch]; !ok || len(out) != 3 {
			t.Fatalf("deliveryByChannel: want exactly one entry per channel, got %+v", m.DeliveryByChannel)
		}
	}
	return out
}

func sortedQueue(q []apiclient.QueueDepth) []apiclient.QueueDepth {
	c := slices.Clone(q)
	slices.SortFunc(c, func(a, b apiclient.QueueDepth) int { return strings.Compare(string(a.Channel), string(b.Channel)) })
	return c
}

// ---- more API instances ----

// withEnv returns env with each key in over replaced (or added).
func withEnv(env []string, over map[string]string) []string {
	out := make([]string, 0, len(env)+len(over))
	for _, kv := range env {
		k, _, _ := strings.Cut(kv, "=")
		if _, ok := over[k]; ok {
			continue
		}
		out = append(out, kv)
	}
	for k, v := range over {
		out = append(out, k+"="+v)
	}
	return out
}

// envValue returns the value of key in env.
func envValue(env []string, key string) string {
	for _, kv := range env {
		if k, v, ok := strings.Cut(kv, "="); ok && k == key {
			return v
		}
	}
	return ""
}

// startServe starts one more `app serve` against this stack's database and
// Valkey (any variable can be overridden), on a fresh port, and waits until
// /readyz answers 200. The returned stack speaks HTTP to the new instance; its
// Logs("serve") are the new instance's. It stops when the test ends.
func (s *stack) startServe(t *testing.T, over map[string]string) *stack {
	t.Helper()
	port := freePort(t)
	o := map[string]string{"APP_HTTP_ADDR": "127.0.0.1:" + strconv.Itoa(port)}
	for k, v := range over {
		o[k] = v
	}
	env := withEnv(s.env, o)
	peer := &stack{
		URL:         "http://127.0.0.1:" + strconv.Itoa(port),
		API:         newAPIClient(t, "http://127.0.0.1:"+strconv.Itoa(port)),
		DSN:         envValue(env, "APP_DATABASE_DSN"),
		DB:          s.DB,
		Provider:    s.Provider,
		ProviderURL: s.ProviderURL,
		t:           t,
		env:         env,
	}
	c := startChild(t, "serve", env, "serve")
	peer.serve = c
	deadline := time.Now().Add(60 * time.Second)
	for {
		if c.exited() {
			t.Fatalf("second app serve exited before ready:\n%s", strings.Join(c.logs.lines(), "\n"))
		}
		if st, _, err := peer.readyz(); err == nil && st == http.StatusOK {
			return peer
		}
		if time.Now().After(deadline) {
			t.Fatalf("second app serve not ready within 60s:\n%s", strings.Join(c.logs.lines(), "\n"))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// healthz is GET /healthz through the typed client with a 5 s bound; it
// returns the status and the Health body (an error when there is none).
func (s *stack) healthz() (int, apiclient.Health, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := s.API.HealthWithResponse(ctx)
	if err != nil {
		return 0, apiclient.Health{}, err
	}
	if r.JSON200 == nil {
		return r.StatusCode(), apiclient.Health{}, fmt.Errorf("not a Health body: %s", show(r))
	}
	return r.StatusCode(), *r.JSON200, nil
}

// readyz is GET /readyz through the typed client with a 5 s bound; it returns
// the status and the Health body of a 200 or a 503 (an error otherwise).
func (s *stack) readyz() (int, apiclient.Health, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := s.API.ReadyWithResponse(ctx)
	if err != nil {
		return 0, apiclient.Health{}, err
	}
	switch {
	case r.JSON200 != nil:
		return r.StatusCode(), *r.JSON200, nil
	case r.JSON503 != nil:
		return r.StatusCode(), *r.JSON503, nil
	}
	return r.StatusCode(), apiclient.Health{}, fmt.Errorf("not a Health body: %s", show(r))
}

// ---- TCP proxy ----

// tcpProxy forwards a local port to target. Close drops the listener and every
// open connection, which to its clients looks like the dependency going down.
type tcpProxy struct {
	Addr   string // host:port clients connect to
	ln     net.Listener
	target string

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	once   sync.Once
	wg     sync.WaitGroup
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &tcpProxy{Addr: ln.Addr().String(), ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go p.serve()
	t.Cleanup(p.Close)
	return p
}

func (p *tcpProxy) serve() {
	defer p.wg.Done()
	for {
		down, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			_ = down.Close()
			continue
		}
		if !p.track(down, up) {
			return
		}
		p.wg.Add(2)
		go p.pipe(up, down)
		go p.pipe(down, up)
	}
}

func (p *tcpProxy) track(cs ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
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

func (p *tcpProxy) pipe(dst, src net.Conn) {
	defer p.wg.Done()
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
	p.mu.Lock()
	delete(p.conns, dst)
	delete(p.conns, src)
	p.mu.Unlock()
}

// Close stops accepting, drops every connection and waits for the copiers.
func (p *tcpProxy) Close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		_ = p.ln.Close()
		for c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
}

// ---- WebSocket ----

// dialWS opens GET /v1/ws with the given query parameters (name/value pairs).
// On a refused handshake it returns the HTTP status (0 when none arrived).
func (s *stack) dialWS(kv ...string) (*websocket.Conn, int, error) {
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
	c, res, err := websocket.Dial(ctx, u, nil)
	status := 0
	if res != nil {
		status = res.StatusCode
		if res.Body != nil {
			_ = res.Body.Close()
		}
	}
	if err != nil {
		return nil, status, err
	}
	c.SetReadLimit(1 << 20)
	return c, status, nil
}

// subscribe is dialWS that must succeed; the socket closes when the test ends.
func (s *stack) subscribe(t *testing.T, kv ...string) *websocket.Conn {
	t.Helper()
	c, status, err := s.dialWS(kv...)
	if err != nil {
		t.Fatalf("GET /v1/ws %q: handshake failed (HTTP %d): %v", kv, status, err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

var errWSTimeout = errors.New("no message before the deadline")

// errEventIncomplete is a StatusEvent message without one of its required
// fields `at` and `attempts`, which apiclient.StatusEvent decodes to zero.
var errEventIncomplete = errors.New("StatusEvent without a required field")

// readEvent reads one StatusEvent before deadline. The socket is unusable
// after errWSTimeout (coder/websocket closes it when a read is cancelled).
// A message without `at` or `attempts` (both required) is errEventIncomplete:
// the generated type cannot tell an absent field from its zero value.
func readEvent(c *websocket.Conn, deadline time.Time) (apiclient.StatusEvent, error) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	typ, b, err := c.Read(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return apiclient.StatusEvent{}, errWSTimeout
		}
		return apiclient.StatusEvent{}, fmt.Errorf("read: %w", err)
	}
	if typ != websocket.MessageText {
		return apiclient.StatusEvent{}, fmt.Errorf("want a text message, got type %v: %q", typ, b)
	}
	var e apiclient.StatusEvent
	if err := json.Unmarshal(b, &e); err != nil {
		return apiclient.StatusEvent{}, fmt.Errorf("not a StatusEvent: %q (%w)", b, err)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(b, &keys); err != nil {
		return apiclient.StatusEvent{}, fmt.Errorf("not a StatusEvent: %q (%w)", b, err)
	}
	for _, k := range []string{"at", "attempts"} {
		if _, ok := keys[k]; !ok {
			return apiclient.StatusEvent{}, fmt.Errorf("%w: event %s: `%s` missing: %q", errEventIncomplete, eventString(e), k, b)
		}
	}
	return e, nil
}

// isSnapshot reports whether e is the snapshot of a notificationId subscription.
func isSnapshot(e apiclient.StatusEvent) bool { return deref(e.Snapshot) }

func eventString(e apiclient.StatusEvent) string {
	batch := ""
	if e.BatchId != nil {
		batch = e.BatchId.String()
	}
	return fmt.Sprintf("{id=%s batch=%s status=%s attempts=%d snapshot=%t corr=%q}",
		e.NotificationId, batch, e.Status, e.Attempts, isSnapshot(e), e.CorrelationId)
}

func eventsString(evs []apiclient.StatusEvent) string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = eventString(e)
	}
	return "[" + strings.Join(out, " ") + "]"
}

// collectUntil reads events until done reports true on the events read so
// far, failing the test after timeout with what arrived.
func collectUntil(t *testing.T, c *websocket.Conn, timeout time.Duration, done func([]apiclient.StatusEvent) bool, what string) []apiclient.StatusEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got []apiclient.StatusEvent
	for !done(got) {
		e, err := readEvent(c, deadline)
		if err != nil {
			t.Fatalf("waiting for %s: %v; events so far: %s", what, err, eventsString(got))
		}
		got = append(got, e)
	}
	return got
}

// eventsFor keeps the events about one notification, in arrival order.
func eventsFor(evs []apiclient.StatusEvent, id uuid.UUID) []apiclient.StatusEvent {
	var out []apiclient.StatusEvent
	for _, e := range evs {
		if e.NotificationId == id {
			out = append(out, e)
		}
	}
	return out
}

func statuses(evs []apiclient.StatusEvent) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = string(e.Status)
	}
	return out
}

// reached returns a done func for collectUntil: true once id has an event
// with status want.
func reached(id uuid.UUID, want string) func([]apiclient.StatusEvent) bool {
	return func(evs []apiclient.StatusEvent) bool {
		return slices.ContainsFunc(evs, func(e apiclient.StatusEvent) bool { return e.NotificationId == id && string(e.Status) == want })
	}
}

// requireEventFields checks the StatusEvent required fields on each event
// (readEvent already refused a message without `at` or `attempts`).
func requireEventFields(t *testing.T, evs []apiclient.StatusEvent, correlationID string) {
	t.Helper()
	for _, e := range evs {
		if e.At.IsZero() {
			t.Errorf("event %s: `at` missing", eventString(e))
		}
		if e.CorrelationId != correlationID {
			t.Errorf("event %s: correlationId want %q", eventString(e), correlationID)
		}
	}
}

// ---- provider and tracing ----

// providerRequests waits until the provider double recorded a request with
// Idempotency-Key = id (the notification id) and returns them.
func (s *stack) providerRequests(t *testing.T, id uuid.UUID, timeout time.Duration) []fakeprovider.Request {
	t.Helper()
	var rs []fakeprovider.Request
	if !poll(timeout, func() bool {
		rs = s.Provider.Requests(fakeprovider.Filter{Key: id.String()})
		return len(rs) > 0
	}) {
		t.Fatalf("not within %s: provider double never received notification %s (stored status %q)",
			timeout, id, s.storedStatusQuiet(id))
	}
	return rs
}

// poll is eventually without the failure: it reports whether cond became
// true within timeout, so the caller can describe the final state.
func poll(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *stack) storedStatusQuiet(id uuid.UUID) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var st string
	if err := s.DB.QueryRowContext(ctx, "SELECT status FROM notifications WHERE id = ?", id.String()).Scan(&st); err != nil {
		return "error: " + err.Error()
	}
	return st
}

// waitStatus waits until the stored status is want.
func (s *stack) waitStatus(t *testing.T, id uuid.UUID, want string, timeout time.Duration) {
	t.Helper()
	if !poll(timeout, func() bool { return s.storedStatusQuiet(id) == want }) {
		t.Fatalf("not within %s: notification %s never reached %q (stored status %q)",
			timeout, id, want, s.storedStatusQuiet(id))
	}
}

var traceparentRE = regexp.MustCompile(`^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$`)

// parseTraceparent splits a W3C version-00 traceparent; ok is false when it is
// malformed or carries an all-zero id.
func parseTraceparent(h string) (traceID, spanID string, ok bool) {
	m := traceparentRE.FindStringSubmatch(h)
	if m == nil {
		return "", "", false
	}
	if m[1] == strings.Repeat("0", 32) || m[2] == strings.Repeat("0", 16) {
		return "", "", false
	}
	return m[1], m[2], true
}

func randHex(n int) string {
	return strings.ReplaceAll(newUUID(), "-", "")[:n]
}

// ---- logs ----

// fieldString renders a log field for comparison ("" when absent).
func fieldString(m map[string]any, k string) string {
	v, ok := m[k]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func linesAbout(logs []map[string]any, id string) []map[string]any {
	var out []map[string]any
	for _, l := range logs {
		if fieldString(l, "notification_id") == id {
			out = append(out, l)
		}
	}
	return out
}
