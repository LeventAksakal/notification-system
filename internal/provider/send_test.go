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

	"github.com/LeventAksakal/notification-system/internal/notification"
)

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
	c := New(map[notification.Channel]string{notification.ChannelSMS: srv.URL + "/send"}, time.Second, 4)

	tid, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	sid, _ := trace.SpanIDFromHex("b7ad6b7169203331")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
	res, err := c.Send(ctx, Message{Channel: notification.ChannelSMS, To: "+905551234567", Content: "hi ✓", Key: "id-1", CorrelationID: "corr-1"})
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
	c := New(map[notification.Channel]string{notification.ChannelPush: srv.URL}, 50*time.Millisecond, 1)
	_, err := c.Send(context.Background(), Message{Channel: notification.ChannelPush, Key: "k"})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	if v := Classify(0, err, nil); v.Outcome != notification.OutcomeTransient {
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
	c := New(map[notification.Channel]string{notification.ChannelEmail: srv.URL}, time.Second, 1)
	if _, err := c.Send(context.Background(), Message{Channel: notification.ChannelEmail, Key: "k"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Send(context.Background(), Message{Channel: notification.ChannelEmail, Key: "k"}); err == nil {
		t.Fatal("second call: want an error, got a response")
	}
	if got := n.Load(); got != 2 {
		t.Errorf("requests at the server: want 2 (no silent replay), got %d", got)
	}
}
