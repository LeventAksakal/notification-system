//go:build integration

package acceptance

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
	"github.com/LeventAksakal/notification-system/test/support"
)

// stackOptions configures startStack.
type stackOptions struct {
	// Workers is how many `app worker` processes start with the stack. Default
	// 0: intake tests deliver nothing unless they ask.
	Workers int
	// Env overrides any variable given to the children.
	Env map[string]string
	// Provider configures the provider double.
	Provider []fakeprovider.Option
}

// stack is one running system: a fresh database, `app serve`, any workers,
// and the provider double.
type stack struct {
	URL         string                         // base URL of `app serve`, no trailing slash
	API         *apiclient.ClientWithResponses // typed client generated from api/openapi.yaml, on URL
	DSN         string                         // DSN of this stack's database (as given to the children)
	DB          *sql.DB
	Provider    *fakeprovider.Server
	ProviderURL string // base URL of the provider double

	t       *testing.T
	env     []string
	mu      sync.Mutex
	serve   *child
	workers []*child // index i is worker-(i+1); kept after a kill so its logs stay readable
}

var dbSeq atomic.Int64

// freshDB creates an empty database on the shared MySQL and returns a pool
// on it and its DSN (go-sql-driver format, no parameters). Nothing is
// migrated. The database is dropped when the test ends.
func freshDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	in := shared(t)
	base := fmt.Sprintf("root:%s@tcp(%s)/", mysqlRootPassword, in.mysqlAddr)
	root, err := sql.Open("mysql", base)
	if err != nil {
		t.Fatalf("open mysql root: %v", err)
	}
	defer func() { _ = root.Close() }()
	name := fmt.Sprintf("acc_%d_%d", os.Getpid(), dbSeq.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := root.ExecContext(ctx, "CREATE DATABASE "+name+" CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	dsn := base + name
	db, err := sql.Open("mysql", dsn+"?parseTime=true&loc=UTC")
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	db.SetMaxOpenConns(20)
	t.Cleanup(func() {
		_ = db.Close()
		r, err := sql.Open("mysql", base)
		if err != nil {
			return
		}
		defer func() { _ = r.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = r.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name)
	})
	return db, dsn
}

// startStack starts a complete system on a fresh, migrated database with an
// empty Valkey, waits until /readyz answers 200, and starts o.Workers workers.
// Everything stops when the test ends.
func startStack(t *testing.T, o stackOptions) *stack {
	t.Helper()
	in := shared(t)
	flushValkey(t, in.valkeyAddr)
	db, dsn := freshDB(t)
	providerURL, provider := support.Provider(t, o.Provider...)

	port := freePort(t)
	s := &stack{
		URL:         "http://127.0.0.1:" + strconv.Itoa(port),
		API:         newAPIClient(t, "http://127.0.0.1:"+strconv.Itoa(port)),
		DSN:         dsn,
		DB:          db,
		Provider:    provider,
		ProviderURL: providerURL,
		t:           t,
	}
	vars := map[string]string{
		"APP_HTTP_ADDR":                   "127.0.0.1:" + strconv.Itoa(port),
		"APP_DATABASE_DSN":                dsn,
		"APP_VALKEY_ADDR":                 in.valkeyAddr,
		"APP_PROVIDER_URL_SMS":            providerURL + "/send",
		"APP_PROVIDER_URL_EMAIL":          providerURL + "/send",
		"APP_PROVIDER_URL_PUSH":           providerURL + "/send",
		"APP_PROVIDER_TIMEOUT":            "5s",
		"APP_MAX_ATTEMPTS":                "5",
		"APP_RETRY_BASE":                  "1s",
		"APP_RETRY_CAP":                   "5m",
		"APP_RATE_LIMIT_PER_SECOND":       strconv.Itoa(dlRateLimit),
		"APP_RATE_LIMIT_HEADROOM_PERCENT": strconv.Itoa(dlRateHeadroomPct),
		"APP_PRIORITY_WEIGHTS":            "70,20,10",
		"APP_LEASE_DURATION":              "30s",
		"APP_REAP_INTERVAL":               "5s",
		"APP_WORKER_CONCURRENCY":          "32",
		"APP_BREAKER_FAILURES":            "5", // the service default (design digest: Envoy's consecutive_5xx)
		"APP_BREAKER_COOLDOWN":            "5s",
		"APP_IDEMPOTENCY_RETENTION":       "24h",
		"APP_WS_SEND_BUFFER":              "256",
	}
	maps.Copy(vars, o.Env)
	s.env = childEnv(vars)

	runApp(t, s.env, "migrate")

	s.serve = startChild(t, "serve", s.env, "serve")
	t.Cleanup(func() { s.stopAll() })

	deadline := time.Now().Add(60 * time.Second)
	for {
		if s.serve.exited() {
			t.Fatalf("app serve exited before ready:\n%s", strings.Join(s.serve.logs.lines(), "\n"))
		}
		if st, _, err := s.readyz(); err == nil && st == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("app serve not ready within 60s:\n%s", strings.Join(s.serve.logs.lines(), "\n"))
		}
		time.Sleep(100 * time.Millisecond)
	}
	for range o.Workers {
		s.StartWorker()
	}
	return s
}

// StartWorker starts one more `app worker` and returns its number (1-based);
// its logs are role "worker-<n>".
func (s *stack) StartWorker() int {
	s.t.Helper()
	s.mu.Lock()
	n := len(s.workers) + 1
	s.mu.Unlock()
	c := startChild(s.t, "worker-"+strconv.Itoa(n), s.env, "worker")
	s.mu.Lock()
	s.workers = append(s.workers, c)
	s.mu.Unlock()
	return n
}

// KillWorker kills worker n (1-based) at once, as a crash would.
func (s *stack) KillWorker(n int) {
	s.t.Helper()
	s.mu.Lock()
	if n < 1 || n > len(s.workers) {
		s.mu.Unlock()
		s.t.Fatalf("KillWorker(%d): no such worker", n)
		return
	}
	c := s.workers[n-1]
	s.mu.Unlock()
	c.kill()
}

// Logs returns the JSON log lines of a role: "serve", "worker" (every
// worker), or "worker-<n>". Lines that are not JSON objects are skipped.
func (s *stack) Logs(role string) []map[string]any {
	var out []map[string]any
	for _, l := range s.LogLines(role) {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil && m != nil {
			out = append(out, m)
		}
	}
	return out
}

// LogLines returns the raw stdout and stderr lines of a role (see Logs).
func (s *stack) LogLines(role string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, c := range s.children() {
		if c.role == role || (role == "worker" && strings.HasPrefix(c.role, "worker-")) {
			out = append(out, c.logs.lines()...)
		}
	}
	return out
}

// children returns every child ever started; the caller holds s.mu.
func (s *stack) children() []*child {
	out := []*child{}
	if s.serve != nil {
		out = append(out, s.serve)
	}
	for _, w := range s.workers {
		if w != nil {
			out = append(out, w)
		}
	}
	return out
}

func (s *stack) stopAll() {
	s.mu.Lock()
	cs := s.children()
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, c := range cs {
		wg.Go(c.stop)
	}
	wg.Wait()
	if s.t.Failed() {
		for _, c := range cs {
			lines := c.logs.lines()
			if len(lines) > 60 {
				lines = lines[len(lines)-60:]
			}
			s.t.Logf("--- last %d log lines of %s ---\n%s", len(lines), c.role, strings.Join(lines, "\n"))
		}
	}
}

// newAPIClient returns the typed client generated from api/openapi.yaml
// (test/apiclient) for the API at baseURL. Every well-formed request goes
// through it, so a contract change breaks the test build, not a test run.
// Each request is bounded at 60 s, as the raw helpers are.
func newAPIClient(t *testing.T, baseURL string) *apiclient.ClientWithResponses {
	t.Helper()
	c, err := apiclient.NewClientWithResponses(baseURL, apiclient.WithHTTPClient(&http.Client{Timeout: 60 * time.Second}))
	if err != nil {
		t.Fatalf("API client for %s: %v", baseURL, err)
	}
	return c
}

// typedAnswer is what every generated XxxResponse offers.
type typedAnswer interface {
	StatusCode() int
	GetBody() []byte
}

// show renders a typed answer for a failure message.
func show(r typedAnswer) string {
	return fmt.Sprintf("HTTP %d %s", r.StatusCode(), truncate(r.GetBody()))
}

// answered fails the test on a transport or decode error of a typed call and
// returns its answer: answered(t)(s.API.CancelNotificationWithResponse(ctx, id)).
func answered(t *testing.T) func(typedAnswer, error) typedAnswer {
	t.Helper()
	return func(r typedAnswer, err error) typedAnswer {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
}

// ptr returns a pointer to v, for the optional fields of the generated types.
func ptr[T any](v T) *T { return &v }

// deref returns *p, or the zero value when the field was absent.
func deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// jsonOf renders v as JSON for a failure message.
func jsonOf(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%+v", v)
	}
	return truncate(b)
}

// response is one HTTP answer, fully read.
type response struct {
	Status int
	Header http.Header
	Body   []byte
}

func (r *response) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, v); err != nil {
		t.Fatalf("decode response (status %d): %v\nbody: %s", r.Status, err, truncate(r.Body))
	}
}

func (r *response) object(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	r.decode(t, &m)
	return m
}

func (r *response) String() string {
	return fmt.Sprintf("HTTP %d %s", r.Status, truncate(r.Body))
}

// StatusCode and GetBody make a raw response a typedAnswer too.
func (r *response) StatusCode() int { return r.Status }
func (r *response) GetBody() []byte { return r.Body }

// do sends one raw request to the API. body is JSON-encoded unless it is nil,
// a []byte or a string (sent as is). kv are header name/value pairs. Only for
// what the typed client cannot express (malformed JSON, a non-UUID path id, a
// repeated query parameter, an over-limit body) or paths outside the contract.
func (s *stack) do(t *testing.T, method, path string, body any, kv ...string) *response {
	t.Helper()
	r, err := s.send(method, path, body, kv...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// send is do without a *testing.T, for use from other goroutines.
func (s *stack) send(method, path string, body any, kv ...string) (*response, error) {
	if len(kv)%2 != 0 {
		return nil, fmt.Errorf("odd number of header arguments %q", kv)
	}
	var rdr io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		rdr = bytes.NewReader(b)
	case string:
		rdr = strings.NewReader(b)
	default:
		buf, err := json.Marshal(b)
		if err != nil {
			return nil, fmt.Errorf("encode body: %w", err)
		}
		rdr = bytes.NewReader(buf)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, s.URL+path, rdr)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	if rdr != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(kv); i += 2 {
		req.Header.Set(kv[i], kv[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("%s %s: read body: %w", method, path, err)
	}
	return &response{Status: res.StatusCode, Header: res.Header, Body: b}, nil
}

func (s *stack) postJSON(t *testing.T, path string, body any, kv ...string) *response {
	t.Helper()
	return s.do(t, http.MethodPost, path, body, kv...)
}

func (s *stack) getJSON(t *testing.T, path string, kv ...string) *response {
	t.Helper()
	return s.do(t, http.MethodGet, path, nil, kv...)
}

// eventually polls cond until it is true or timeout passes, then fails.
func eventually(t *testing.T, timeout time.Duration, cond func() bool, format string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not within %s: "+format, append([]any{timeout}, args...)...)
		}
		<-tick.C
	}
}

// consistently polls cond for the whole duration and fails the first time it
// is false. It is the bounded wait for "this never happens".
func consistently(t *testing.T, d time.Duration, cond func() bool, format string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(d)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if !cond() {
			t.Fatalf("broke within %s: "+format, append([]any{d}, args...)...)
		}
		if time.Now().After(deadline) {
			return
		}
		<-tick.C
	}
}

// runApp runs `app <args>` to completion with env and fails the test if it
// does not exit 0 within a minute. It returns the combined output.
func runApp(t *testing.T, env []string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, appBin, args...)
	cmd.Dir = repoRoot
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("app %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// childEnv is the parent environment without any APP_* variable, plus vars.
func childEnv(vars map[string]string) []string {
	var env []string
	// The children inherit the test's environment (PATH, TEMP, Docker
	// settings) minus every APP_* variable, so only the stack's settings
	// configure them; this is not a settings read.
	for _, kv := range os.Environ() { //nolint:forbidigo // child process environment, not a setting

		if strings.HasPrefix(strings.ToUpper(kv), "APP_") {
			continue
		}
		env = append(env, kv)
	}
	for _, k := range slices.Sorted(maps.Keys(vars)) {
		env = append(env, k+"="+vars[k])
	}
	return env
}

func flushValkey(t *testing.T, addr string) {
	t.Helper()
	c, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatalf("valkey client: %v", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Do(ctx, c.B().Flushall().Build()).Error(); err != nil {
		t.Fatalf("valkey FLUSHALL: %v", err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func truncate(b []byte) string {
	if len(b) > 2000 {
		return string(b[:2000]) + "...(truncated)"
	}
	return string(b)
}

// newUUID returns a random version-4 UUID string.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// child is one `app` process with its captured output.
type child struct {
	role string
	cmd  *exec.Cmd
	logs *logBuffer
	done chan struct{}
	once sync.Once
}

func startChild(t *testing.T, role string, env []string, args ...string) *child {
	t.Helper()
	lb := &logBuffer{}
	// Not t.Context(): it is cancelled before cleanups run, which would kill
	// the child before stop() can interrupt it. The child's lifetime is owned
	// by stop/kill, so its context never ends.
	cmd := exec.CommandContext(context.Background(), appBin, args...)
	cmd.Dir = repoRoot
	cmd.Env = env
	cmd.Stdout = lb.writer()
	cmd.Stderr = lb.writer()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start app %s: %v", strings.Join(args, " "), err)
	}
	c := &child{role: role, cmd: cmd, logs: lb, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(c.done)
	}()
	t.Cleanup(c.stop) // also stopped by stack.stopAll; stop is idempotent
	return c
}

func (c *child) exited() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// kill ends the process at once and waits for it.
func (c *child) kill() {
	_ = c.cmd.Process.Kill()
	<-c.done
}

// stop asks the process to exit (an interrupt; on Windows, which cannot
// deliver one to a child, a kill) and waits; after 15 s it kills.
func (c *child) stop() {
	c.once.Do(func() {
		if c.exited() {
			return
		}
		if runtime.GOOS == "windows" {
			c.kill()
			return
		}
		_ = c.cmd.Process.Signal(os.Interrupt)
		select {
		case <-c.done:
		case <-time.After(15 * time.Second):
			c.kill()
		}
	})
}

// logBuffer collects lines from any number of writers; safe for concurrent use.
type logBuffer struct {
	mu  sync.Mutex
	all []string
}

func (b *logBuffer) lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.all)
}

func (b *logBuffer) writer() io.Writer { return &lineWriter{buf: b} }

// lineWriter splits one stream into lines; a partial line waits for its end.
type lineWriter struct {
	buf     *logBuffer
	partial []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.partial = append(w.partial, p...)
	for {
		i := bytes.IndexByte(w.partial, '\n')
		if i < 0 {
			break
		}
		line := strings.TrimRight(string(w.partial[:i]), "\r")
		w.partial = w.partial[i+1:]
		w.buf.mu.Lock()
		w.buf.all = append(w.buf.all, line)
		w.buf.mu.Unlock()
	}
	return len(p), nil
}
