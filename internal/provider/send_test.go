package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"slices"
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

// silentListener accepts connections and never writes to them, so a TLS
// client's handshake waits for a ServerHello that never comes.
func silentListener(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

// The provider client speaks HTTP/1.1 even to a provider that offers HTTP/2:
// h2 multiplexes calls on one connection, which breaks the per-call write
// count behind ErrNotSent and lets one failed call's Close cut the others.
func TestSendSpeaksHTTP11ToAProviderOfferingHTTP2(t *testing.T) {
	var proto atomic.Value
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proto.Store(r.Proto)
		w.WriteHeader(http.StatusAccepted)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	if !slices.Contains(srv.TLS.NextProtos, "h2") {
		t.Fatalf("the test server offers %v, want h2 among them", srv.TLS.NextProtos)
	}
	c := testClient(t, map[notification.Channel]string{notification.ChannelSMS: srv.URL}, time.Second, 1)
	trustTLS(c, srv)
	if _, err := c.Send(context.Background(), Message{Channel: notification.ChannelSMS, Key: "k"}); err != nil {
		t.Fatal(err)
	}
	if got := proto.Load(); got != "HTTP/1.1" {
		t.Errorf("the provider saw %v, want HTTP/1.1", got)
	}
}

// A TLSHandshakeTimeout of 0 is no limit beyond the call's own deadline, as
// in the stock transport, not a handshake that fails at once.
func TestSendOverTLSWithNoHandshakeTimeout(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	defer srv.Close()
	c := testClient(t, map[notification.Channel]string{notification.ChannelSMS: srv.URL}, time.Second, 1)
	trustTLS(c, srv)
	c.http.Transport.(*http.Transport).TLSHandshakeTimeout = 0
	if res, err := c.Send(context.Background(), Message{Channel: notification.ChannelSMS, Key: "k"}); err != nil || res.Status != http.StatusAccepted {
		t.Fatalf("status %d, err %v; want 202", res.Status, err)
	}
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
		{"a TLS handshake that fails (untrusted certificate) is a provider fault", func(t *testing.T) (string, func(*Client)) {
			srv := httptest.NewUnstartedServer(http.HandlerFunc(hangingHandler))
			srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the server's "handshake error" line is expected
			srv.StartTLS()
			t.Cleanup(srv.Close)
			return srv.URL, nil // the client does not trust the test certificate
		}},
		{"a TLS handshake that never completes is a provider fault", func(t *testing.T) (string, func(*Client)) {
			return "https://" + silentListener(t) + "/send", nil
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
		calls  = 300 // per round: contention enough for both stages
		rounds = 20  // bound on the search; see the log for the rounds a run took
		warm   = 32  // idle connections, so calls obtain one at once
	)
	var mu sync.Mutex
	received := map[string]int{}
	var sawNotSent, sawAfterWrite bool
	timeout := 200 * time.Microsecond
	for round := 0; round < rounds && (!sawNotSent || !sawAfterWrite); round++ {
		// One server per round, so the round can wait for every request it
		// will ever see before counting.
		var open atomic.Int64
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			mu.Lock()
			received[r.Header.Get("Idempotency-Key")]++
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		}))
		srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
			switch s {
			case http.StateNew:
				open.Add(1)
			case http.StateClosed, http.StateHijacked:
				open.Add(-1)
			}
		}
		srv.Start()
		urls := map[notification.Channel]string{notification.ChannelSMS: srv.URL}
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
		// Every client connection is now closed or idle; close the idle ones.
		// The server closes its side only after it read the EOF, so behind
		// every byte that arrived: when no connection is open, every request
		// that reached it has been counted. srv.Close alone is not enough: it
		// drops an idle connection whose request bytes it has not read yet.
		c.http.CloseIdleConnections()
		for deadline := time.Now().Add(5 * time.Second); open.Load() > 0; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) { // 5 s: a round's connections close in milliseconds
				t.Fatalf("round %d: %d server connections still open after 5s", round, open.Load())
			}
		}
		srv.Close()
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

// The leave-by tests' moments. ahead: 15 ms, far above the microseconds from a
// call's start to its GetConn and above a coarse clock's tick (the Windows
// monotonic clock ticks in milliseconds), so the call is on time when it
// asks for a connection. margin: 5 ms past the excused wait, far above the
// microseconds between Send's own hooks and the test's, so the write comes
// after the armed moment.
const (
	ahead  = 15 * time.Millisecond
	margin = 5 * time.Millisecond
)

// stallAfterGotConn returns ctx whose calls, once they have their
// connection, wait until past their leave-by moment plus the time they
// waited for the connection (and margin): the descheduled worker between
// the transport's handoff and the first write. Send's own hooks run first
// (httptrace composes the newer trace before the older), so the check is
// armed before the stall.
func stallAfterGotConn(ctx context.Context, by time.Time) context.Context {
	var getAt time.Time
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn: func(string) { getAt = time.Now() },
		GotConn: func(httptrace.GotConnInfo) {
			until := by.Add(time.Since(getAt) + margin)
			for now := time.Now(); !now.After(until); now = time.Now() {
				<-time.After(until.Sub(now) + time.Microsecond)
			}
		},
	})
}

// A call on time when it asks for a connection whose first write then comes
// after its leave-by moment (WithLeaveBy) plus the time it waited for the
// connection writes nothing: the provider never sees it, the call is
// ErrNotSent and ErrTooLate (a slot given up, never sent late: decision
// 0009), and the client's next call goes out normally. On a fresh
// connection only the dial is excused, not a stall after it.
func TestSendRefusesAFirstWriteAfterTheLeaveByMoment(t *testing.T) {
	future := func() time.Time { return time.Now().Add(time.Hour) } // an hour: no call here lasts that long
	cases := []struct {
		name    string
		tls     bool
		warm    bool // an earlier call leaves an idle connection the timed call takes
		stall   bool // the call stalls after GotConn past its excused moment
		leaveBy func() time.Time
		refused bool
	}{
		{"a stall on a pooled connection is refused", false, true, true, nil, true},
		{"a stall on a pooled TLS connection is refused", true, true, true, nil, true},
		{"a stall after the dial on a fresh connection is refused", false, false, true, nil, true},
		{"a stall after the dial on a fresh TLS connection is refused", true, false, true, nil, true},
		{"a moment still ahead lets the request leave", false, true, false, future, false},
		{"no moment lets the request leave", false, true, false, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newCountingProvider(t, tc.tls, false)
			// 1 s: far above a local call, never reached; 1 connection: the
			// timed call takes the warm one.
			c := p.client(t, time.Second, 1)
			if tc.warm {
				if _, err := p.send(context.Background(), c, "warm"); err != nil {
					t.Fatalf("warm-up: %v", err)
				}
			}
			ctx := context.Background()
			switch {
			case tc.stall:
				by := time.Now().Add(ahead)
				ctx = stallAfterGotConn(WithLeaveBy(ctx, by), by)
			case tc.leaveBy != nil:
				ctx = WithLeaveBy(ctx, tc.leaveBy())
			}
			res, err := p.send(ctx, c, "timed")
			if tc.refused {
				if !errors.Is(err, ErrNotSent) || !errors.Is(err, ErrTooLate) {
					t.Fatalf("status %d, err %v; want ErrNotSent and ErrTooLate", res.Status, err)
				}
			} else if err != nil || res.Status != http.StatusAccepted {
				t.Fatalf("status %d, err %v; want 202", res.Status, err)
			}
			if res, err := p.send(context.Background(), c, "next"); err != nil || res.Status != http.StatusAccepted {
				t.Fatalf("the next call: status %d, err %v; want 202", res.Status, err)
			}
			want := 1
			if tc.refused {
				want = 0
			}
			if got, next := p.received("timed"), p.received("next"); got != want || next != 1 {
				t.Errorf("the provider saw the timed request %d time(s) and the next %d; want %d and 1", got, next, want)
			}
		})
	}
}

// A call already past its moment when Send starts is refused before it
// takes or dials a connection: nothing reaches the provider and no
// connection is opened or closed for it, so the pooled one serves the next
// call.
func TestSendRefusesACallAlreadyLateWithoutAConnection(t *testing.T) {
	for _, warm := range []bool{true, false} {
		t.Run(fmt.Sprintf("pooled connection waiting %v", warm), func(t *testing.T) {
			p := newCountingProvider(t, false, false)
			c := p.client(t, time.Second, 1) // 1 s: never reached; 1 connection: the next call reuses it
			var dials atomic.Int64
			tr := c.http.Transport.(*http.Transport)
			dial := tr.DialContext
			tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				dials.Add(1)
				return dial(ctx, network, addr)
			}
			if warm {
				if _, err := p.send(context.Background(), c, "warm"); err != nil {
					t.Fatalf("warm-up: %v", err)
				}
			}
			before := dials.Load()
			// A millisecond behind the call's start: past by any later clock reading.
			_, err := p.send(WithLeaveBy(context.Background(), time.Now().Add(-time.Millisecond)), c, "late")
			if !errors.Is(err, ErrNotSent) || !errors.Is(err, ErrTooLate) {
				t.Fatalf("err %v, want ErrNotSent and ErrTooLate", err)
			}
			if n := dials.Load() - before; n != 0 || p.received("late") != 0 {
				t.Fatalf("%d dials and %d requests at the provider for the late call, want 0 and 0", n, p.received("late"))
			}
			if res, err := p.send(context.Background(), c, "next"); err != nil || res.Status != http.StatusAccepted {
				t.Fatalf("the next call: status %d, err %v; want 202", res.Status, err)
			}
			if warm && dials.Load() != before {
				t.Errorf("the next call dialed: the pooled connection was closed for the late one")
			}
		})
	}
}

// A dial (or TLS handshake) slower than the tolerance must not refuse an
// on-time call it was made for: if it did, every call on a new connection
// would be refused, its connection closed, and the pool would never refill
// (verifier finding, 16 Nov 2026). Calls whose every connection is dialed
// past their moment all leave; a stall after the dial, or on a pooled
// connection, is still refused.
func TestSendLetsACallLeaveOnAConnectionDialedPastItsMoment(t *testing.T) {
	// 30 ms: three times the whole tolerance at 95 per second (one interval,
	// 10.53 ms, minus the clock uncertainty), and twice ahead, so each dial
	// ends past an on-time call's moment.
	const dialDelay = 30 * time.Millisecond
	const calls = 5 // several: a refused dial would fail each, not just the first
	t.Run("every on-time call on a connection dialed past its moment leaves", func(t *testing.T) {
		p := newCountingProvider(t, false, true) // the provider closes each connection: every call dials
		c := p.client(t, time.Second, 1)         // 1 s: never reached; 1 connection: one at a time
		slowDial(c, dialDelay)
		for i := range calls {
			key := fmt.Sprintf("call-%d", i)
			if res, err := p.send(WithLeaveBy(context.Background(), time.Now().Add(ahead)), c, key); err != nil || res.Status != http.StatusAccepted {
				t.Fatalf("%s: status %d, err %v; want 202", key, res.Status, err)
			}
			if n := p.received(key); n != 1 {
				t.Fatalf("%s: the provider saw it %d time(s), want 1", key, n)
			}
		}
	})
	t.Run("a stall after the slow dial is refused", func(t *testing.T) {
		p := newCountingProvider(t, false, true) // every call dials
		c := p.client(t, time.Second, 1)         // 1 s: never reached; 1 connection: one at a time
		slowDial(c, dialDelay)
		by := time.Now().Add(ahead)
		_, err := p.send(stallAfterGotConn(WithLeaveBy(context.Background(), by), by), c, "stalled")
		if !errors.Is(err, ErrNotSent) || !errors.Is(err, ErrTooLate) {
			t.Fatalf("err %v, want ErrNotSent and ErrTooLate", err)
		}
		if n := p.received("stalled"); n != 0 {
			t.Errorf("the provider saw it %d time(s), want 0", n)
		}
	})
	t.Run("a stalled call on the pooled connection is refused", func(t *testing.T) {
		p := newCountingProvider(t, false, false)
		c := p.client(t, time.Second, 1) // 1 s: never reached; 1 connection: the stalled call takes the pooled one
		slowDial(c, dialDelay)
		if res, err := p.send(WithLeaveBy(context.Background(), time.Now().Add(ahead)), c, "dialed"); err != nil || res.Status != http.StatusAccepted {
			t.Fatalf("dialed: status %d, err %v; want 202", res.Status, err)
		}
		by := time.Now().Add(ahead)
		_, err := p.send(stallAfterGotConn(WithLeaveBy(context.Background(), by), by), c, "pooled")
		if !errors.Is(err, ErrTooLate) {
			t.Fatalf("pooled: err %v, want ErrTooLate", err)
		}
		if n := p.received("pooled"); n != 0 {
			t.Errorf("pooled: the provider saw it %d time(s), want 0", n)
		}
	})
}

// countingProvider is a test provider that accepts every call and counts
// them by Idempotency-Key.
type countingProvider struct {
	srv *httptest.Server
	tls bool
	mu  sync.Mutex
	got map[string]int
}

// newCountingProvider starts one; closeEach makes it close every connection
// after its answer, as a provider dropping idle connections.
func newCountingProvider(t *testing.T, tls, closeEach bool) *countingProvider {
	t.Helper()
	p := &countingProvider{tls: tls, got: map[string]int{}}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		p.mu.Lock()
		p.got[r.Header.Get("Idempotency-Key")]++
		p.mu.Unlock()
		if closeEach {
			w.Header().Set("Connection", "close")
		}
		w.WriteHeader(http.StatusAccepted)
	})
	if tls {
		p.srv = httptest.NewTLSServer(h)
	} else {
		p.srv = httptest.NewServer(h)
	}
	t.Cleanup(p.srv.Close)
	return p
}

func (p *countingProvider) client(t *testing.T, timeout time.Duration, conns int) *Client {
	c := testClient(t, map[notification.Channel]string{notification.ChannelSMS: p.srv.URL}, timeout, conns)
	if p.tls {
		trustTLS(c, p.srv)
	}
	return c
}

func (p *countingProvider) send(ctx context.Context, c *Client, key string) (Response, error) {
	return c.Send(ctx, Message{Channel: notification.ChannelSMS, Key: key})
}

func (p *countingProvider) received(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.got[key]
}

// slowDial makes every new connection of c take d longer to dial.
func slowDial(c *Client, d time.Duration) {
	tr := c.http.Transport.(*http.Transport)
	dial := tr.DialContext // New's: it wraps the connection in a writeMarkConn
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		tm := time.NewTimer(d)
		defer tm.Stop()
		select {
		case <-tm.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return dial(ctx, network, addr)
	}
}
