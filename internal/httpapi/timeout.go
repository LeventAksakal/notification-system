package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// DefaultRequestTimeout is APP_REQUEST_TIMEOUT's default.
const DefaultRequestTimeout = 10 * time.Second

// errRequestTimeout is the cause of a request cut by its deadline.
var errRequestTimeout = fmt.Errorf("request deadline: %w", context.DeadlineExceeded)

// withTimeout gives every request a deadline of d, except the health
// endpoints (/healthz answers at once whatever the database does; /readyz
// has its own 2 s). A handler that runs out of time on the database answers
// 503 itself (store.IsUnavailable); one that does not return when the
// deadline fires (a driver call that ignores its context) still gets its
// 503 written then. The handler runs in a goroutine and writes to a buffer;
// ServeHTTP owns that goroutine and waits for it before it returns, after the
// 503 has gone out. The connection is then closed, not reused.
func withTimeout(d time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeoutCause(r.Context(), d, errRequestTimeout)
		defer cancel()
		tw := &timeoutWriter{h: make(http.Header)}
		done := make(chan any, 1)
		go func() { // owned here: ServeHTTP waits on done before it returns
			defer func() { done <- recover() }()
			next.ServeHTTP(tw, r.WithContext(ctx))
		}()
		select {
		case p := <-done:
			if p != nil {
				panic(p) // as if the handler had run here: net/http recovers it
			}
			tw.flushTo(w)
		case <-ctx.Done():
			if !tw.timeout() {
				// The handler finished just as the deadline fired.
				p := <-done
				if p != nil {
					panic(p)
				}
				tw.flushTo(w)
				return
			}
			w.Header().Set("Connection", "close")
			slog.WarnContext(r.Context(), "request deadline passed; answered 503", "method", r.Method, "path", r.URL.Path, "err", context.Cause(ctx))
			writeError(w, errUnavailable())
			_ = http.NewResponseController(w).Flush()
			if p := <-done; p != nil {
				panic(p)
			}
		}
	})
}

// timeoutWriter buffers a handler's response until it is done; once the
// request has timed out, everything it writes is dropped.
type timeoutWriter struct {
	h http.Header // the handler's; read by flushTo only once it is done

	mu       sync.Mutex
	buf      bytes.Buffer
	status   int
	timedOut bool
}

func (tw *timeoutWriter) Header() http.Header { return tw.h }

func (tw *timeoutWriter) Write(p []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut {
		// The 503 has gone out; the late answer is dropped. No error: Huma
		// treats a failed write as a bug and panics.
		return len(p), nil
	}
	if tw.status == 0 {
		tw.status = http.StatusOK
	}
	return tw.buf.Write(p)
}

func (tw *timeoutWriter) WriteHeader(code int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut || tw.status != 0 {
		return
	}
	tw.status = code
}

// timeout marks the request timed out unless the handler already wrote a
// response; it reports whether it did.
func (tw *timeoutWriter) timeout() bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.status != 0 {
		return false
	}
	tw.timedOut = true
	return true
}

// flushTo copies the buffered response to w; the handler is done.
func (tw *timeoutWriter) flushTo(w http.ResponseWriter) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	dst := w.Header()
	for k, vs := range tw.h {
		dst[k] = vs
	}
	if tw.status == 0 {
		tw.status = http.StatusOK
	}
	if tw.status >= 200 && tw.status != http.StatusNoContent && tw.status != http.StatusNotModified {
		dst.Set("Content-Length", strconv.Itoa(tw.buf.Len()))
	}
	w.WriteHeader(tw.status)
	_, _ = w.Write(tw.buf.Bytes())
}
