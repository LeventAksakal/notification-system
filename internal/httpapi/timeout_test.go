package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/LeventAksakal/notification-system/internal/store"
)

// blockingMetrics blocks until its context ends, then (like a driver that
// does not watch its context) for linger more, and fails with the cause.
type blockingMetrics struct{ linger time.Duration }

func (b blockingMetrics) Read(ctx context.Context, _ time.Duration) (store.Metrics, error) {
	<-ctx.Done()
	time.Sleep(b.linger)
	return store.Metrics{}, context.Cause(ctx)
}

func TestARequestOutOfTimeIs503InTime(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	const timeout = 100 * time.Millisecond
	for _, tc := range []struct {
		name   string
		linger time.Duration
	}{
		{"the handler gives up at the deadline", 0},
		{"the handler hangs past the deadline", 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := NewServer(nil, nil, Options{Metrics: blockingMetrics{tc.linger}, RequestTimeout: timeout}).Handler()
			if err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewServer(h)
			start := time.Now()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/v1/metrics", nil)
			if err != nil {
				t.Fatal(err)
			}
			res, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(res.Body)
			_ = res.Body.Close()
			took := time.Since(start)
			if err != nil {
				t.Fatal(err)
			}
			var e Error
			if res.StatusCode != http.StatusServiceUnavailable || json.Unmarshal(body, &e) != nil || e.Code != "unavailable" ||
				res.Header.Get("Retry-After") != "1" {
				t.Fatalf("got %d %q Retry-After %q", res.StatusCode, body, res.Header.Get("Retry-After"))
			}
			// A loaded machine may be late; the hanging handler is 2 s.
			if took > timeout+time.Second {
				t.Fatalf("answered after %s, timeout %s", took, timeout)
			}
			srv.Close() // waits for the handler: no goroutine outlives the request
		})
	}
}

func TestTheHealthEndpointsHaveNoRequestDeadline(t *testing.T) {
	var deadlines atomic.Int32
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.Context().Deadline(); ok {
			deadlines.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	})
	h := withTimeout(time.Minute, next)
	for _, path := range []string{"/healthz", "/readyz", "/v1/notifications"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	}
	if n := deadlines.Load(); n != 1 {
		t.Fatalf("%d of 3 requests had a deadline, want only /v1/notifications", n)
	}
}

func TestAPromptAnswerPassesThroughTheDeadline(t *testing.T) {
	h := withTimeout(time.Minute, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Test", "yes")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("made"))
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/notifications", nil))
	if rec.Code != http.StatusCreated || rec.Body.String() != "made" || rec.Header().Get("X-Test") != "yes" || rec.Header().Get("Content-Length") != "4" {
		t.Fatalf("got %d %q %v", rec.Code, rec.Body, rec.Header())
	}
}
