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
	c := testClient(t, map[notification.Channel]string{notification.ChannelSMS: srv.URL + "/send"}, time.Second, 4)

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
	c := testClient(t, map[notification.Channel]string{notification.ChannelPush: srv.URL}, 50*time.Millisecond, 1)
	_, err := c.Send(context.Background(), Message{Channel: notification.ChannelPush, Key: "k"})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	if v := Classify(0, err, nil); v.Outcome != notification.OutcomeTransient {
		t.Errorf("timeout classified %s", v.Outcome)
	}
}

// testClient is New whose idle connections close when the test ends (goleak).
func testClient(t *testing.T, urls map[notification.Channel]string, timeout time.Duration, conns int) *Client {
	t.Helper()
	c := New(urls, timeout, conns)
	t.Cleanup(c.http.CloseIdleConnections)
	return c
}

// trustTLS makes c trust srv's test certificate.
func trustTLS(c *Client, srv *httptest.Server) {
	c.http.Transport.(*http.Transport).TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
}

// hangingHandler reads the request, then answers only when the caller left.
func hangingHandler(_ http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	<-r.Context().Done()
}

// A failed call is ErrNotSent only when it obtained a connection and wrote
// nothing to it: the worker then gives the job back instead of recording an
// attempt the provider never saw (decision 0005). A call that obtained no
// connection is a provider fault and stays a recorded attempt, so a stopped
// provider still opens an outage and still exhausts budgets.
func TestSendTellsACallThatNeverLeftFromOneThatMayHave(t *testing.T) {
	const timeout = 100 * time.Millisecond // short: every case ends in it or sooner
	cases := []struct {
		name  string
		setup func(t *testing.T) (url string, tune func(*Client))
	}{
		{"a request the provider received but did not answer may have left", func(t *testing.T) (string, func(*Client)) {
			srv := httptest.NewServer(http.HandlerFunc(hangingHandler))
			t.Cleanup(srv.Close)
			return srv.URL, nil
		}},
		{"the same over TLS may have left", func(t *testing.T) (string, func(*Client)) {
			srv := httptest.NewTLSServer(http.HandlerFunc(hangingHandler))
			t.Cleanup(srv.Close)
			return srv.URL, func(c *Client) { trustTLS(c, srv) }
		}},
		{"a refused dial is a provider fault, not a call that never left", func(t *testing.T) (string, func(*Client)) {
			ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := ln.Addr().String()
			_ = ln.Close()
			return "http://" + addr + "/send", nil
		}},
		{"a dial that never completes is a provider fault, not a call that never left", func(*testing.T) (string, func(*Client)) {
			return "http://192.0.2.1/send", func(c *Client) {
				c.http.Transport.(*http.Transport).DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url, tune := tc.setup(t)
			c := testClient(t, map[notification.Channel]string{notification.ChannelSMS: url}, timeout, 1)
			if tune != nil {
				tune(c)
			}
			_, err := c.Send(context.Background(), Message{Channel: notification.ChannelSMS, Key: "k"})
			if err == nil {
				t.Fatal("want an error")
			}
			if errors.Is(err, ErrNotSent) {
				t.Fatalf("got ErrNotSent, want a recorded attempt (err: %v)", err)
			}
			if v := Classify(0, err, nil); v.Outcome != notification.OutcomeTransient || !v.ProviderFault {
				t.Errorf("classified %s, provider fault %v; want a transient provider fault", v.Outcome, v.ProviderFault)
			}
		})
	}
}

// What a failed call obtained decides: nothing written since it got its
// connection(s) is "not sent", over plain TCP and over TLS (whose
// close_notify on Close is no request byte); a write started on any of them,
// or no connection at all, is not.
func TestConnUseCountsRequestWritesSinceTheCallGotItsConnections(t *testing.T) {
	pipe := func(t *testing.T) *writeMarkConn {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
		go func() { _, _ = io.Copy(io.Discard, server) }()
		return &writeMarkConn{Conn: client}
	}
	write := func(t *testing.T, c net.Conn) {
		if _, err := c.Write([]byte("POST /send")); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("nothing written since the call got its connection is not sent", func(t *testing.T) {
		c := pipe(t)
		write(t, c) // an earlier call on the same connection
		var u connUse
		u.gotConn(c)
		if !u.notSent() {
			t.Error("want not sent")
		}
	})
	t.Run("a write started after the call got its connection may have left", func(t *testing.T) {
		c := pipe(t)
		var u connUse
		u.gotConn(c)
		write(t, c)
		if u.notSent() {
			t.Error("want may have left")
		}
	})
	t.Run("a write on a second connection the call obtained may have left", func(t *testing.T) {
		first, second := pipe(t), pipe(t)
		var u connUse
		u.gotConn(first)
		u.gotConn(second)
		write(t, second)
		if u.notSent() {
			t.Error("want may have left")
		}
	})
	t.Run("no connection obtained is a failed dial, not 'not sent'", func(t *testing.T) {
		var u connUse
		if u.notSent() {
			t.Error("want a recorded attempt")
		}
	})
	t.Run("a TLS connection with nothing written is not sent though Close writes close_notify", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(hangingHandler))
		defer srv.Close()
		c := testClient(t, map[notification.Channel]string{notification.ChannelSMS: srv.URL}, time.Second, 1)
		trustTLS(c, srv)
		conn, err := c.http.Transport.(*http.Transport).DialTLSContext(t.Context(), "tcp", srv.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		var u connUse
		u.gotConn(conn)
		if !u.notSent() {
			t.Error("want not sent")
		}
	})
}

// Under contention, calls on warm connections time out before their write
// and after it. Whatever the stage, a call reported ErrNotSent never reached
// the server, so the worker's attempt rows match the requests the provider
// saw. Both stages must occur, or the property was not exercised: each round
// shortens or lengthens the timeout until both have.
func TestSendNeverReportsNotSentForARequestTheServerReceived(t *testing.T) {
	const (
		calls  = 300                    // per round: contention enough for both stages
		rounds = 20                     // bound on the search; see the log for the rounds a run took
		quiet  = 200 * time.Millisecond // late arrivals after the clients gave up
		warm   = 32                     // idle connections, so calls obtain one at once
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
	var sawNotSent, sawAfterWrite bool
	timeout := 200 * time.Microsecond
	for round := 0; round < rounds && (!sawNotSent || !sawAfterWrite); round++ {
		c := testClient(t, urls, time.Second, warm)
		var wg sync.WaitGroup
		for i := range warm { // warm the pool on the long timeout
			wg.Go(func() {
				if _, err := c.Send(context.Background(), Message{Channel: notification.ChannelSMS, Key: fmt.Sprintf("w%d-%d", round, i)}); err != nil {
					t.Errorf("warm-up: %v", err)
				}
			})
		}
		wg.Wait()
		c.timeout = timeout
		notSent, failed := make([]bool, calls), make([]bool, calls)
		for i := range calls {
			wg.Go(func() {
				_, err := c.Send(context.Background(), Message{Channel: notification.ChannelSMS, Key: fmt.Sprintf("%d-%d", round, i)})
				notSent[i], failed[i] = errors.Is(err, ErrNotSent), err != nil
			})
		}
		wg.Wait()
		c.http.CloseIdleConnections()
		time.Sleep(quiet)
		mu.Lock()
		var nNotSent, nAfterWrite int
		for i := range calls {
			got := received[fmt.Sprintf("%d-%d", round, i)]
			switch {
			case notSent[i] && got > 0:
				t.Errorf("round %d call %d: reported ErrNotSent, but the server received it %d time(s)", round, i, got)
			case notSent[i]:
				nNotSent++
			case failed[i] && got > 0:
				nAfterWrite++
			}
		}
		mu.Unlock()
		t.Logf("round %d, timeout %s: %d not sent, %d failed after the write", round, timeout, nNotSent, nAfterWrite)
		sawNotSent, sawAfterWrite = sawNotSent || nNotSent > 0, sawAfterWrite || nAfterWrite > 0
		switch {
		case !sawNotSent:
			timeout = max(timeout/2, time.Microsecond) // the floor: below a microsecond calls end before they get a connection
		case !sawAfterWrite:
			timeout *= 2
		}
	}
	if !sawNotSent || !sawAfterWrite {
		t.Fatalf("after %d rounds: a not-sent call %v, a call failed after its write %v; the property was not exercised", rounds, sawNotSent, sawAfterWrite)
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
	c := testClient(t, map[notification.Channel]string{notification.ChannelEmail: srv.URL}, time.Second, 1)
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
