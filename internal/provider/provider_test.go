package provider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/LeventAksakal/notification-system/internal/core"
)

func TestClassify(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	hdr := func(v string) http.Header { return http.Header{"Retry-After": {v}} }
	cases := []struct {
		name       string
		status     int
		err        error
		h          http.Header
		want       core.AttemptOutcome
		retryAfter time.Duration
		fault      bool
	}{
		{"202 is accepted", 202, nil, nil, core.OutcomeAccepted, 0, false},
		{"200 is accepted", 200, nil, nil, core.OutcomeAccepted, 0, false},
		{"network error is transient", 0, errors.New("connection refused"), nil, core.OutcomeTransient, 0, true},
		{"timeout is transient", 0, ErrTimeout, nil, core.OutcomeTransient, 0, true},
		{"408 is transient", 408, nil, nil, core.OutcomeTransient, 0, true},
		{"425 is transient", 425, nil, nil, core.OutcomeTransient, 0, true},
		{"429 with seconds", 429, nil, hdr("2"), core.OutcomeTransient, 2 * time.Second, true},
		{"503 with an HTTP date", 503, nil, hdr(now.Add(90 * time.Second).Format(http.TimeFormat)), core.OutcomeTransient, 90 * time.Second, true},
		{"503 with a past date waits nothing extra", 503, nil, hdr(now.Add(-time.Minute).Format(http.TimeFormat)), core.OutcomeTransient, 0, true},
		{"503 with garbage Retry-After", 503, nil, hdr("soon"), core.OutcomeTransient, 0, true},
		{"500 is transient", 500, nil, nil, core.OutcomeTransient, 0, true},
		{"599 is transient", 599, nil, nil, core.OutcomeTransient, 0, true},
		{"huge Retry-After is capped", 429, nil, hdr("99999999999"), core.OutcomeTransient, maxRetryAfter, true},
		{"400 is permanent", 400, nil, nil, core.OutcomePermanent, 0, false},
		{"401 is permanent", 401, nil, nil, core.OutcomePermanent, 0, false},
		{"404 is permanent", 404, nil, nil, core.OutcomePermanent, 0, false},
		{"422 is permanent", 422, nil, nil, core.OutcomePermanent, 0, false},
		{"Retry-After on a 400 is ignored", 400, nil, hdr("5"), core.OutcomePermanent, 0, false},
		{"a redirect is permanent", 302, nil, nil, core.OutcomePermanent, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := classifyAt(now, c.status, c.err, c.h)
			if v.Outcome != c.want || v.RetryAfter != c.retryAfter || v.ProviderFault != c.fault {
				t.Errorf("got %+v, want outcome %s retryAfter %s fault %v", v, c.want, c.retryAfter, c.fault)
			}
			if (v.Outcome == core.OutcomeAccepted) != (v.Error == "") {
				t.Errorf("error text %q for outcome %s", v.Error, v.Outcome)
			}
		})
	}
}

func TestSendWireFormat(t *testing.T) {
	var got struct {
		body map[string]any
		h    http.Header
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got.body)
		got.h = r.Header.Clone()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m-42","status":"accepted","timestamp":"2026-11-13T12:00:00Z"}`))
	}))
	defer srv.Close()
	c := New(map[core.Channel]string{core.ChannelSMS: srv.URL + "/send"}, time.Second, 4)

	tid, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	sid, _ := trace.SpanIDFromHex("b7ad6b7169203331")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
	res, err := c.Send(ctx, Message{Channel: core.ChannelSMS, To: "+905551234567", Content: "hi ✓", Key: "id-1", CorrelationID: "corr-1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != 202 || res.MessageID != "m-42" {
		t.Errorf("response: %+v", res)
	}
	if len(got.body) != 3 || got.body["to"] != "+905551234567" || got.body["channel"] != "sms" || got.body["content"] != "hi ✓" {
		t.Errorf("body: %v", got.body)
	}
	if got.h.Get("Idempotency-Key") != "id-1" || got.h.Get("X-Correlation-ID") != "corr-1" {
		t.Errorf("headers: %v", got.h)
	}
	if tp := got.h.Get("Traceparent"); tp != "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" {
		t.Errorf("traceparent: %q", tp)
	}
}

func TestSendTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	c := New(map[core.Channel]string{core.ChannelPush: srv.URL}, 50*time.Millisecond, 1)
	_, err := c.Send(context.Background(), Message{Channel: core.ChannelPush, Key: "k"})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	if v := Classify(0, err, nil); v.Outcome != core.OutcomeTransient {
		t.Errorf("timeout classified %s", v.Outcome)
	}
}

// TestSendIsNotReplayedByTheTransport pins decision 0006's choice: when a
// reused connection dies before any response byte, net/http would resend a
// POST carrying Idempotency-Key on its own. One call must be one request.
func TestSendIsNotReplayedByTheTransport(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		if n.Add(1) == 1 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close() // the reused connection dies without an answer
		}
	}))
	defer srv.Close()
	c := New(map[core.Channel]string{core.ChannelEmail: srv.URL}, time.Second, 1)
	if _, err := c.Send(context.Background(), Message{Channel: core.ChannelEmail, Key: "k"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Send(context.Background(), Message{Channel: core.ChannelEmail, Key: "k"}); err == nil {
		t.Fatal("second call: want an error, got a response")
	}
	if got := n.Load(); got != 2 {
		t.Errorf("requests at the server: want 2 (no silent replay), got %d", got)
	}
}
