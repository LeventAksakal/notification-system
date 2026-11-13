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

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

// ---- API shapes (api/openapi.yaml) ----

type latencyMs struct {
	P50 int `json:"p50"`
	P95 int `json:"p95"`
	P99 int `json:"p99"`
}

type queueDepth struct {
	Channel            string `json:"channel"`
	Pending            int    `json:"pending"`
	Scheduled          int    `json:"scheduled"`
	Processing         int    `json:"processing"`
	OldestPendingAgeMs int    `json:"oldestPendingAgeMs"`
}

type deliveryTotals struct {
	Sent           int       `json:"sent"`
	Failed         int       `json:"failed"`
	SuccessRate    float64   `json:"successRate"`
	FailureRate    float64   `json:"failureRate"`
	Attempts       int       `json:"attempts"`
	FailedAttempts int       `json:"failedAttempts"`
	LatencyMs      latencyMs `json:"latencyMs"`
}

// deliveryStats is DeliveryStats; encoding/json flattens the embedded fields.
type deliveryStats struct {
	Channel string `json:"channel"`
	deliveryTotals
}

type metrics struct {
	GeneratedAt       time.Time       `json:"generatedAt"`
	WindowSeconds     int             `json:"windowSeconds"`
	QueueDepth        int             `json:"queueDepth"`
	Queue             []queueDepth    `json:"queue"`
	Totals            statusCounts    `json:"totals"`
	Delivery          deliveryTotals  `json:"delivery"`
	DeliveryByChannel []deliveryStats `json:"deliveryByChannel"`
}

type health struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

// statusEvent is one WebSocket message. Pointer fields tell "absent" from zero.
type statusEvent struct {
	NotificationID string     `json:"notificationId"`
	BatchID        string     `json:"batchId"`
	Status         string     `json:"status"`
	Attempts       *int       `json:"attempts"`
	Error          string     `json:"error"`
	At             *time.Time `json:"at"`
	CorrelationID  string     `json:"correlationId"`
	Snapshot       bool       `json:"snapshot"`
}

func (e statusEvent) String() string {
	a := "<absent>"
	if e.Attempts != nil {
		a = strconv.Itoa(*e.Attempts)
	}
	return fmt.Sprintf("{id=%s batch=%s status=%s attempts=%s snapshot=%t corr=%q}",
		e.NotificationID, e.BatchID, e.Status, a, e.Snapshot, e.CorrelationID)
}

// ---- metrics ----

// metrics fetches GET /v1/metrics with the given raw query ("" for none) and
// requires 200.
func (s *stack) metrics(t *testing.T, query string) metrics {
	t.Helper()
	p := "/v1/metrics"
	if query != "" {
		p += "?" + query
	}
	r := s.getJSON(t, p)
	if r.Status != http.StatusOK {
		t.Fatalf("GET %s: want 200, got %s", p, r)
	}
	var m metrics
	r.decode(t, &m)
	return m
}

// queueByChannel indexes m.Queue by channel and fails unless it holds exactly
// one entry for each of sms, email and push.
func queueByChannel(t *testing.T, m metrics) map[string]queueDepth {
	t.Helper()
	out := map[string]queueDepth{}
	for _, q := range m.Queue {
		if _, dup := out[q.Channel]; dup {
			t.Fatalf("queue has two entries for %q: %+v", q.Channel, m.Queue)
		}
		out[q.Channel] = q
	}
	for _, ch := range []string{"sms", "email", "push"} {
		if _, ok := out[ch]; !ok || len(out) != 3 {
			t.Fatalf("queue: want exactly one entry per channel (sms, email, push), got %+v", m.Queue)
		}
	}
	return out
}

func deliveryByChannel(t *testing.T, m metrics) map[string]deliveryStats {
	t.Helper()
	out := map[string]deliveryStats{}
	for _, d := range m.DeliveryByChannel {
		if _, dup := out[d.Channel]; dup {
			t.Fatalf("deliveryByChannel has two entries for %q: %+v", d.Channel, m.DeliveryByChannel)
		}
		out[d.Channel] = d
	}
	for _, ch := range []string{"sms", "email", "push"} {
		if _, ok := out[ch]; !ok || len(out) != 3 {
			t.Fatalf("deliveryByChannel: want exactly one entry per channel, got %+v", m.DeliveryByChannel)
		}
	}
	return out
}

func sortedQueue(q []queueDepth) []queueDepth {
	c := slices.Clone(q)
	slices.SortFunc(c, func(a, b queueDepth) int { return strings.Compare(a.Channel, b.Channel) })
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
		if st, err := quickGet(peer.URL + "/readyz"); err == nil && st == http.StatusOK {
			return peer
		}
		if time.Now().After(deadline) {
			t.Fatalf("second app serve not ready within 60s:\n%s", strings.Join(c.logs.lines(), "\n"))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// probe is GET url with a 5 s bound; it decodes a Health body when present.
func probe(url string) (int, health, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, health{}, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, health{}, err
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return res.StatusCode, health{}, err
	}
	var h health
	if err := json.Unmarshal(b, &h); err != nil {
		return res.StatusCode, health{}, fmt.Errorf("not a Health body: %q", truncate(b))
	}
	return res.StatusCode, h, nil
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

// readEvent reads one StatusEvent before deadline. The socket is unusable
// after errWSTimeout (coder/websocket closes it when a read is cancelled).
func readEvent(c *websocket.Conn, deadline time.Time) (statusEvent, error) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	typ, b, err := c.Read(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return statusEvent{}, errWSTimeout
		}
		return statusEvent{}, fmt.Errorf("read: %w", err)
	}
	if typ != websocket.MessageText {
		return statusEvent{}, fmt.Errorf("want a text message, got type %v: %q", typ, b)
	}
	var e statusEvent
	if err := json.Unmarshal(b, &e); err != nil {
		return statusEvent{}, fmt.Errorf("not a StatusEvent: %q (%w)", b, err)
	}
	return e, nil
}

// collectUntil reads events until done reports true on the events read so
// far, failing the test after timeout with what arrived.
func collectUntil(t *testing.T, c *websocket.Conn, timeout time.Duration, done func([]statusEvent) bool, what string) []statusEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got []statusEvent
	for !done(got) {
		e, err := readEvent(c, deadline)
		if err != nil {
			t.Fatalf("waiting for %s: %v; events so far: %v", what, err, got)
		}
		got = append(got, e)
	}
	return got
}

// eventsFor keeps the events about one notification, in arrival order.
func eventsFor(evs []statusEvent, id string) []statusEvent {
	var out []statusEvent
	for _, e := range evs {
		if e.NotificationID == id {
			out = append(out, e)
		}
	}
	return out
}

func statuses(evs []statusEvent) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Status
	}
	return out
}

// reached returns a done func for collectUntil: true once id has an event
// with status want.
func reached(id, want string) func([]statusEvent) bool {
	return func(evs []statusEvent) bool {
		return slices.ContainsFunc(evs, func(e statusEvent) bool { return e.NotificationID == id && e.Status == want })
	}
}

// requireEventFields checks the StatusEvent required fields on each event.
func requireEventFields(t *testing.T, evs []statusEvent, correlationID string) {
	t.Helper()
	for _, e := range evs {
		if e.At == nil || e.At.IsZero() {
			t.Errorf("event %v: `at` missing", e)
		}
		if e.Attempts == nil {
			t.Errorf("event %v: `attempts` missing", e)
		}
		if e.CorrelationID != correlationID {
			t.Errorf("event %v: correlationId want %q", e, correlationID)
		}
	}
}

// ---- provider and tracing ----

// providerRequests waits until the provider double recorded a request with
// Idempotency-Key = id (the notification id) and returns them.
func (s *stack) providerRequests(t *testing.T, id string, timeout time.Duration) []fakeprovider.Request {
	t.Helper()
	var rs []fakeprovider.Request
	if !poll(timeout, func() bool {
		rs = s.Provider.Requests(fakeprovider.Filter{Key: id})
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

func (s *stack) storedStatusQuiet(id string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var st string
	if err := s.DB.QueryRowContext(ctx, "SELECT status FROM notifications WHERE id = ?", id).Scan(&st); err != nil {
		return "error: " + err.Error()
	}
	return st
}

// waitStatus waits until the stored status is want.
func (s *stack) waitStatus(t *testing.T, id, want string, timeout time.Duration) {
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
