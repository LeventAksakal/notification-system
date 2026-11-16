package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
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

// A failed call is ErrNotSent only when no byte of it can have left: the
// worker then gives the job back instead of recording an attempt the
// provider never saw (decision 0005).
func TestSendTellsACallThatNeverLeftFromOneThatMayHave(t *testing.T) {
	const timeout = 50 * time.Millisecond // short: every case ends in it or sooner
	cases := []struct {
		name    string
		url     func(t *testing.T) string
		dial    func(ctx context.Context, network, addr string) (net.Conn, error) // nil: the default dialer
		notSent bool
	}{
		{"a request the provider received but did not answer may have left", func(t *testing.T) string {
			srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				<-r.Context().Done()
			}))
			t.Cleanup(srv.Close)
			return srv.URL
		}, nil, false},
		{"a call that never got a connection did not leave", func(*testing.T) string { return "http://192.0.2.1/send" },
			func(ctx context.Context, _, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }, true},
		{"a refused dial did not leave", func(t *testing.T) string {
			ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := ln.Addr().String()
			_ = ln.Close()
			return "http://" + addr + "/send"
		}, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := New(map[notification.Channel]string{notification.ChannelSMS: tc.url(t)}, timeout, 1)
			if tc.dial != nil {
				c.http.Transport.(*http.Transport).DialContext = tc.dial
			}
			_, err := c.Send(context.Background(), Message{Channel: notification.ChannelSMS, Key: "k"})
			if err == nil {
				t.Fatal("want an error")
			}
			if got := errors.Is(err, ErrNotSent); got != tc.notSent {
				t.Fatalf("ErrNotSent = %v, want %v (err: %v)", got, tc.notSent, err)
			}
		})
	}
}

// Under contention many calls time out at every stage (dialling, before the
// write, after it). Whatever the stage, a call reported ErrNotSent never
// reached the server, so the worker's attempt rows match the requests the
// provider saw.
func TestSendNeverReportsNotSentForARequestTheServerReceived(t *testing.T) {
	const (
		calls = 400                    // enough that every timeout stage occurs on a loaded runner
		quiet = 300 * time.Millisecond // late arrivals after the clients gave up
	)
	var mu sync.Mutex
	received := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		received[r.Header.Get("Idempotency-Key")]++
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	urls := map[notification.Channel]string{notification.ChannelSMS: srv.URL}
	notSent, failed := make([]bool, calls), make([]bool, calls)
	var wg sync.WaitGroup
	for i := range calls {
		// Timeouts from 0.5 ms to 20 ms, spread over 40 steps: from "before
		// any dial" to "answered" on a loaded runner (measured: every stage
		// occurs with GOMAXPROCS=1).
		c := New(urls, time.Duration(500+i%40*500)*time.Microsecond, 4)
		wg.Go(func() {
			_, err := c.Send(context.Background(), Message{Channel: notification.ChannelSMS, Key: fmt.Sprint(i)})
			notSent[i], failed[i] = errors.Is(err, ErrNotSent), err != nil
		})
	}
	wg.Wait()
	time.Sleep(quiet)
	mu.Lock()
	defer mu.Unlock()
	var nNotSent, nReceived, nFailedReceived int
	for i := range calls {
		got := received[fmt.Sprint(i)]
		nReceived += got
		if failed[i] && !notSent[i] && got > 0 {
			nFailedReceived++ // timed out after the write: may have left, and did
		}
		if notSent[i] {
			nNotSent++
			if got > 0 {
				t.Errorf("call %d: reported ErrNotSent, but the server received it %d time(s)", i, got)
			}
		}
	}
	t.Logf("%d calls: %d not sent, %d received (%d of them by calls that failed after the write)", calls, nNotSent, nReceived, nFailedReceived)
}

// A connection obtained by a call but never written to before it failed
// counts as not sent; one write started on it counts as sent.
func TestConnUseCountsWritesStartedSinceTheCallGotItsConnection(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go func() { _, _ = io.Copy(io.Discard, server) }()
	wc := &writeMarkConn{Conn: client}
	if _, err := wc.Write([]byte("an earlier request on the same connection")); err != nil {
		t.Fatal(err)
	}
	var idle connUse
	idle.gotConn(wc)
	if idle.mayHaveLeft() {
		t.Error("nothing written since the call got the connection: want not sent")
	}

	client2, server2 := net.Pipe()
	defer func() { _ = server2.Close() }()
	go func() { _, _ = io.Copy(io.Discard, server2) }()
	wc2 := &writeMarkConn{Conn: client2}
	var written connUse
	written.gotConn(wc2)
	if _, err := wc2.Write([]byte("POST /send")); err != nil {
		t.Fatal(err)
	}
	if !written.mayHaveLeft() {
		t.Error("a write started after the call got the connection: want may have left")
	}
	var none connUse
	if none.mayHaveLeft() {
		t.Error("no connection obtained: want not sent")
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
