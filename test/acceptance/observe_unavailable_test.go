//go:build integration

package acceptance

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// R15: Health check endpoint - "unready when the database or Valkey is down"
// - and the API's answer while the database is down. CONTRACT (api/openapi.yaml
// 1.3.0, Conventions): "`503 unavailable` with `Retry-After`: the database
// cannot be reached; nothing was stored." Finding 1 of the 14 Nov fault run:
// with MySQL killed, POST /v1/notifications answered 500 internal.
// This `serve` reaches MySQL only through a linkProxy (as TestR15ReadyzMySQLDown
// does with its proxy); closing it makes the port refuse connections, and
// bringing it back on the same port is MySQL coming back.
func TestR15APIUnavailableWhileMySQLDown(t *testing.T) {
	const limit = 10 * time.Second
	s := startStack(t, stackOptions{})
	n := s.create(t, msg("sms", "before the database outage"))
	b := s.createBatch(t, []map[string]any{msg("email", "batch before the database outage")})

	mysqlAddr := shared(t).mysqlAddr
	p := newLinkProxy(t, mysqlAddr)
	dsn := strings.Replace(s.DSN, "tcp("+mysqlAddr+")", "tcp("+p.Addr+")", 1)
	if dsn == s.DSN {
		t.Fatalf("cannot point DSN %q at the proxy", s.DSN)
	}
	peer := s.startServe(t, map[string]string{"APP_DATABASE_DSN": dsn})

	healthz := func(when string) {
		t.Helper()
		if st, h, err := probe(peer.URL + "/healthz"); err != nil || st != http.StatusOK || h.Status != "ok" {
			t.Errorf("/healthz %s: want 200 ok, got %d %+v (%v)", when, st, h, err)
		}
	}
	healthz("before the outage")

	p.down(outageClosed)
	waitReadyz(t, peer, 30*time.Second, "kept answering 200 after MySQL went away", func(st int) bool { return st != http.StatusOK })

	create := msg("push", "during the database outage")
	ops := []struct {
		name, method, path string
		body               any
	}{
		{"create", http.MethodPost, "/v1/notifications", create},
		{"read by id", http.MethodGet, "/v1/notifications/" + n.ID, nil},
		{"list", http.MethodGet, "/v1/notifications?channel=sms", nil},
		{"batch read", http.MethodGet, "/v1/batches/" + b.ID, nil},
	}
	for _, op := range ops {
		t.Run(op.name+" answers 503 unavailable", func(t *testing.T) {
			r, took, err := unavailableDo(peer, op.method, op.path, op.body, 30*time.Second)
			if err != nil {
				t.Fatalf("%s %s with MySQL down: %v (after %s)", op.method, op.path, err, took.Round(time.Millisecond))
			}
			t.Logf("%s %s with MySQL down: %s in %s; Retry-After %q", op.method, op.path, r, took.Round(time.Millisecond), r.Header.Get("Retry-After"))
			if took > limit {
				t.Errorf("answered after %s, want within %s", took.Round(time.Millisecond), limit)
			}
			wantUnavailable(t, r)
		})
		healthz("while MySQL is down (after " + op.name + ")")
	}

	if !p.up(t) {
		t.Skipf("could not listen on %s again to bring MySQL back; recovery step skipped", p.Addr)
	}
	waitReadyz(t, peer, 30*time.Second, "never answered 200 after MySQL came back", func(st int) bool { return st == http.StatusOK })
	var last *response
	if !poll(limit, func() bool {
		r, _, err := unavailableDo(peer, http.MethodPost, "/v1/notifications", create, 10*time.Second)
		if err == nil {
			last = r
		}
		return err == nil && r.Status == http.StatusCreated
	}) {
		t.Fatalf("after MySQL came back (/readyz 200) the same create did not succeed within %s; last %v", limit, last)
	}
	healthz("after MySQL came back")
}

// R15: Health check endpoint - "unready when the database or Valkey is down"
// - and the API's answer while the database HANGS. CONTRACT (api/openapi.yaml
// 1.3.0, Conventions): "`503 unavailable` with `Retry-After`: the database
// cannot be reached or did not answer in time"; APP_REQUEST_TIMEOUT is the
// longest a request may take before the API answers 503. Verifier finding
// (14 Nov): with MySQL accepting connections and never answering (a paused
// container, a black-holed network) the API hung for as long as the client
// waited. Here the linkProxy goes outageUnresponsive: open connections are
// dropped and every new one is accepted and never answered.
func TestR15APIUnavailableWhileMySQLHangs(t *testing.T) {
	const (
		limit         = 8 * time.Second  // APP_REQUEST_TIMEOUT=2s plus slack
		clientTimeout = 20 * time.Second // well above limit: a hang shows as "no answer within"
		healthzLimit  = 2 * time.Second
		recoverLimit  = 60 * time.Second
	)
	s := startStack(t, stackOptions{})
	n := s.create(t, msg("sms", "before the database hang"))

	mysqlAddr := shared(t).mysqlAddr
	p := newLinkProxy(t, mysqlAddr)
	dsn := strings.Replace(s.DSN, "tcp("+mysqlAddr+")", "tcp("+p.Addr+")", 1)
	if dsn == s.DSN {
		t.Fatalf("cannot point DSN %q at the proxy", s.DSN)
	}
	peer := s.startServe(t, map[string]string{"APP_DATABASE_DSN": dsn, "APP_REQUEST_TIMEOUT": "2s"})

	healthz := func(when string) {
		t.Helper()
		r, took, err := unavailableDo(peer, http.MethodGet, "/healthz", nil, clientTimeout)
		if err != nil {
			t.Errorf("/healthz %s: no answer within %s (%v)", when, took.Round(time.Millisecond), err)
			return
		}
		var h health
		if r.Status != http.StatusOK || json.Unmarshal(r.Body, &h) != nil || h.Status != "ok" {
			t.Errorf("/healthz %s: want 200 ok, got %s", when, r)
		}
		if took > healthzLimit {
			t.Errorf("/healthz %s: answered after %s, want within %s", when, took.Round(time.Millisecond), healthzLimit)
		}
	}
	healthz("before the hang")

	p.down(outageUnresponsive)

	create := msg("push", "during the database hang")
	ops := []struct {
		name, method, path string
		body               any
	}{
		{"create", http.MethodPost, "/v1/notifications", create},
		{"read by id", http.MethodGet, "/v1/notifications/" + n.ID, nil},
		{"list", http.MethodGet, "/v1/notifications?channel=sms", nil},
	}
	for _, op := range ops {
		t.Run(op.name+" answers 503 unavailable in time", func(t *testing.T) {
			r, took, err := unavailableDo(peer, op.method, op.path, op.body, clientTimeout)
			if err != nil {
				t.Fatalf("%s %s with MySQL hanging: no answer within %s, want 503 within %s (%v)", op.method, op.path, took.Round(time.Millisecond), limit, err)
			}
			t.Logf("%s %s with MySQL hanging: %s in %s; Retry-After %q", op.method, op.path, r, took.Round(time.Millisecond), r.Header.Get("Retry-After"))
			if took > limit {
				t.Errorf("answered after %s, want within %s", took.Round(time.Millisecond), limit)
			}
			wantUnavailable(t, r)
		})
		healthz("while MySQL hangs (after " + op.name + ")")
	}

	if !p.up(t) {
		t.Fatalf("could not forward to MySQL again on %s", p.Addr)
	}
	var last *response
	var lastErr error
	if !poll(recoverLimit, func() bool {
		r, _, err := unavailableDo(peer, http.MethodPost, "/v1/notifications", create, clientTimeout)
		last, lastErr = r, err
		return err == nil && r.Status == http.StatusCreated
	}) {
		t.Fatalf("after MySQL answered again the same create did not succeed within %s; last %v (%v)", recoverLimit, last, lastErr)
	}
	healthz("after MySQL answered again")
}

// unavailableDo sends one request to peer with a hard client timeout.
func unavailableDo(peer *stack, method, path string, body any, clientTimeout time.Duration) (*response, time.Duration, error) {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		rdr = strings.NewReader(string(buf))
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, peer.URL+path, rdr)
	if err != nil {
		return nil, 0, err
	}
	if rdr != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	start := time.Now()
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, time.Since(start), err
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, time.Since(start), err
	}
	return &response{Status: res.StatusCode, Header: res.Header, Body: b}, time.Since(start), nil
}

// wantUnavailable requires 503 with a JSON Error body of code unavailable and
// a Retry-After header holding a whole number of seconds.
func wantUnavailable(t *testing.T, r *response) {
	t.Helper()
	if r.Status != http.StatusServiceUnavailable {
		t.Errorf("want 503 unavailable, got %s", r)
	}
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		t.Errorf("Content-Type: want application/json, got %q", r.Header.Get("Content-Type"))
	}
	var e apiError
	if err := json.Unmarshal(r.Body, &e); err != nil {
		t.Errorf("want an Error body, got %s (%v)", r, err)
	} else if e.Code != "unavailable" {
		t.Errorf("error code: want %q, got %q (%s)", "unavailable", e.Code, r)
	}
	ra := r.Header.Get("Retry-After")
	if secs, err := strconv.Atoi(strings.TrimSpace(ra)); err != nil || secs < 0 {
		t.Errorf("Retry-After: want a whole number of seconds, got %q", ra)
	}
}
