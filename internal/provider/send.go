// Package provider calls the external notification provider (R16, decisions
// 0006, 0007, 0019) and is the one place that decides what its answer means
// (Classify).
package provider

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/propagation"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// Message is what one provider call carries.
type Message struct {
	Channel       notification.Channel
	To            string
	Content       string
	Key           string // Idempotency-Key: the notification id, identical across attempts
	CorrelationID string
}

// Response is the provider's answer; Status is 0 when none arrived.
type Response struct {
	Status    int
	Header    http.Header
	MessageID string // from a 2xx body; "" when absent
}

// ErrUnknownChannel is returned for a channel without a configured URL.
var ErrUnknownChannel = errors.New("no provider URL for channel")

// ErrTimeout wraps a call that ran past the provider timeout.
var ErrTimeout = errors.New("provider timeout")

// ErrNotSent wraps a failed call that obtained a connection and wrote
// nothing to it before it ended (a timeout or the caller's deadline): the
// provider cannot have seen it, so it is no attempt (decision 0005) and the
// caller gives the job back instead of recording a result. Measured on the
// two-core acceptance runs: a descheduled worker's 500 ms calls ended after
// 710-946 ms with nothing written, and the double counted no request.
// A call that obtained no connection (a refused dial, a DNS failure, a dial
// timeout) is not ErrNotSent: the provider's address did not answer, a
// provider fault the caller records and reports to provider health, so a
// stopped provider still opens an outage and still exhausts budgets.
var ErrNotSent = errors.New("the request never left")

// ErrTooLate wraps, beside ErrNotSent, a call whose request was due to leave
// by a moment (WithLeaveBy) it could no longer keep: the moment had passed
// when Send started, or when the first write came (on a connection the call
// waited for, the moment plus that wait: connUse.got). Nothing reached the
// provider. A slot that cannot be
// kept is given up, never sent late (decision 0009); the check sits at the
// first write because everything before it (the caller's last clock check,
// goroutine scheduling, the transport's connection handoff) can still delay
// the request past its moment.
var ErrTooLate = errors.New("the request would have left after its leave-by moment")

// leaveByKey is the context key of WithLeaveBy.
type leaveByKey struct{}

// WithLeaveBy returns ctx carrying the moment by which a call's request must
// have started its first write; the zero time is none. Send returns
// ErrNotSent and ErrTooLate for a call already past it, without taking a
// connection, and refuses a later first write, excusing only the time the
// call waited for its connection (connUse.got says why). A context value, not a field of Message:
// it is a property of this attempt (its slot), not of the notification, and
// the deadline of ctx is a different moment (the call's end).
func WithLeaveBy(ctx context.Context, by time.Time) context.Context {
	return context.WithValue(ctx, leaveByKey{}, by)
}

// LeaveBy reports the moment WithLeaveBy put in ctx.
func LeaveBy(ctx context.Context) (time.Time, bool) {
	by, ok := ctx.Value(leaveByKey{}).(time.Time)
	return by, ok
}

// maxBody bounds how much of an answer is read.
const maxBody = 64 << 10

// Client sends notifications to the per-channel provider URLs.
type Client struct {
	urls    map[notification.Channel]string
	timeout time.Duration
	http    *http.Client
}

// New returns a client. conns sizes the idle pool per host to the number of
// calls in flight, so connections are reused instead of re-dialled.
func New(urls map[notification.Channel]string, timeout time.Duration, conns int) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = max(conns*len(notification.Channels), 100)
	tr.MaxIdleConnsPerHost = max(conns, 2)
	// HTTP/1.1 only. HTTP/2 multiplexes calls on one connection: one call's
	// writes would move another's count, and closing a failed call's
	// connection (connUse.notSent) would cut every stream on it. A provider
	// call is one small request, so h2 buys nothing here.
	tr.ForceAttemptHTTP2 = false
	tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	// Every connection counts the writes started on it, above TLS: a TLS
	// close_notify written underneath on Close is no request byte. So a
	// failed call can tell "nothing left" from "may have left" (ErrNotSent).
	dial := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &writeMarkConn{Conn: c}, nil
	}
	tr.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		raw, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		cfg := tr.TLSClientConfig.Clone() // read per dial: tests and callers may set it after New
		if cfg == nil {
			cfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		if cfg.ServerName == "" {
			cfg.ServerName, _, _ = net.SplitHostPort(addr)
		}
		cfg.NextProtos = []string{"http/1.1"}
		tc := tls.Client(raw, cfg)
		hctx := ctx
		if d := tr.TLSHandshakeTimeout; d > 0 { // as the transport's own handshake: 0 is no limit beyond ctx
			var cancel context.CancelFunc
			hctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		if err := tc.HandshakeContext(hctx); err != nil {
			_ = raw.Close()
			return nil, err
		}
		// The transport sees a writeMarkConn, not a *tls.Conn, so it leaves
		// http.Response.TLS nil for these calls. Nothing here reads it.
		return &writeMarkConn{Conn: tc}, nil
	}
	return &Client{
		urls:    urls,
		timeout: timeout,
		http: &http.Client{
			Transport: tr,
			// A provider redirect is an answer to classify, not something to follow.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

type body struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// Send makes one provider call. The traceparent header is injected from the
// span in ctx. err is non-nil when no response arrived (network error,
// timeout); a response of any status is not an error.
func (c *Client) Send(ctx context.Context, m Message) (Response, error) {
	url, ok := c.urls[m.Channel]
	if !ok {
		return Response{}, fmt.Errorf("%w: %q", ErrUnknownChannel, m.Channel)
	}
	payload, err := json.Marshal(body{To: m.To, Channel: string(m.Channel), Content: m.Content})
	if err != nil {
		return Response{}, fmt.Errorf("encode body: %w", err)
	}
	cctx, cancel := context.WithTimeoutCause(ctx, c.timeout, ErrTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return Response{}, fmt.Errorf("build request: %w", err)
	}
	// Decision 0006: net/http treats a POST with Idempotency-Key as replayable
	// and resends it by itself once when a reused connection dies before any
	// response byte. That second request would be invisible to our attempt
	// count. Without GetBody the request is not replayable, so one attempt is
	// exactly one request; a lost request surfaces as a transient error and is
	// retried, and recorded, by the worker.
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", m.Key)
	if m.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", m.CorrelationID)
	}
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(req.Header))
	var used connUse
	if by, ok := LeaveBy(ctx); ok && !by.IsZero() {
		if time.Now().After(by) {
			// Already late: taking or dialing a connection only for the
			// first write to be refused would close it, churning the
			// provider's connections for nothing.
			return Response{}, fmt.Errorf("%w: %w", ErrNotSent, ErrTooLate)
		}
		used.leaveBy = by
	}
	req = req.WithContext(httptrace.WithClientTrace(cctx, &httptrace.ClientTrace{GetConn: used.get, GotConn: used.got}))

	res, err := c.http.Do(req)
	if err != nil {
		if errors.Is(context.Cause(cctx), ErrTimeout) && ctx.Err() == nil {
			err = fmt.Errorf("%w after %s", ErrTimeout, c.timeout)
		}
		if used.notSent() {
			if used.refused.Load() {
				err = fmt.Errorf("%w: %w: %w", ErrNotSent, ErrTooLate, err)
			} else {
				err = fmt.Errorf("%w: %w", ErrNotSent, err)
			}
		}
		return Response{}, err
	}
	defer func() { _ = res.Body.Close() }()
	out := Response{Status: res.StatusCode, Header: res.Header}
	raw, err := io.ReadAll(io.LimitReader(res.Body, maxBody))
	if err != nil && res.StatusCode/100 != 2 {
		return out, nil // the status is the answer; a broken error body changes nothing
	}
	if res.StatusCode/100 == 2 {
		var ans struct {
			MessageID string `json:"messageId"`
		}
		if json.Unmarshal(raw, &ans) == nil {
			out.MessageID = ans.MessageID
		}
	}
	// Drain what was not read so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, maxBody))
	return out, nil
}

// writeMarkConn is a provider connection that remembers how many writes were
// started on it. HTTP/1.1 carries one request at a time, so a count that did
// not move while a call held the connection means none of it was written.
type writeMarkConn struct {
	net.Conn
	writes atomic.Int64
	// late is the leave-by check of the call that holds the connection,
	// armed when it got the connection (connUse.got) and taken by the
	// next write, the request's first. Taken, not read: a later write of the
	// same request is not refused (half a request would be a broken one),
	// and the next call on the connection is not judged by this call's
	// moment.
	late atomic.Pointer[lateCheck]
}

// errWriteTooLate is the refused write's error, inside the transport; Send
// reports ErrTooLate from connUse.refused, not from this error, so it does
// not depend on how net/http wraps a write error.
var errWriteTooLate = errors.New("first write after the leave-by moment refused")

// Write counts the write before making it: a write that has started may
// have sent bytes, whether or not it returns. A first write after the
// call's leave-by moment is refused before it is counted: nothing is
// written, so the call is ErrNotSent. The transport closes a connection
// whose write failed, so a refusal leaves nothing behind in the pool.
func (c *writeMarkConn) Write(b []byte) (int, error) {
	if lc := c.late.Swap(nil); lc != nil && time.Now().After(lc.by) {
		lc.refused.Store(true)
		return 0, errWriteTooLate
	}
	c.writes.Add(1)
	return c.Conn.Write(b)
}

// lateCheck is the moment after which the first write on one connection is
// refused, and the flag of the call it belongs to.
type lateCheck struct {
	by      time.Time
	refused *atomic.Bool // connUse.refused
}

// connUse is every connection one call obtained (httptrace.GotConn), with
// its write count at that moment. A call normally obtains one; a transport
// retry on a fresh connection would add another.
type connUse struct {
	mu    sync.Mutex
	conns []usedConn
	// other: a connection that is not a writeMarkConn. Clone keeps
	// ProxyFromEnvironment, and with HTTPS_PROXY set an https call goes
	// through a CONNECT tunnel on which the transport layers TLS itself, so
	// GotConn sees its *tls.Conn. Such a call is never ErrNotSent: recorded,
	// the conservative answer. Proxies stay on because egress through one is
	// a deployment's choice; the compose stack and the tests set none.
	other bool
	// leaveBy is the call's moment (WithLeaveBy); zero: none. Behind a
	// proxy (other) nothing is armed and the request is never refused at the
	// write. Set before the call and only read after, so not under mu.
	leaveBy time.Time
	// getAt is when the call last asked the transport for a connection
	// (get); under mu.
	getAt time.Time
	// refused: a first write of the call was refused for lateness.
	refused atomic.Bool
}

type usedConn struct {
	conn *writeMarkConn
	mark int64
}

// get notes when the call asks the transport for a connection
// (httptrace.GetConn), before any dial starts.
func (u *connUse) get(string) {
	if u.leaveBy.IsZero() {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.getAt = time.Now()
}

// got records the connection and arms the leave-by check on it: its first
// write is refused after the call's moment plus the time the call waited
// for the connection (GetConn to GotConn), and nothing else is excused.
//   - A reused connection waited about nothing: the moment itself.
//   - A connection dialed for the call excuses its dial and TLS handshake:
//     they can outlast the whole tolerance (one interval minus the clock
//     uncertainty, under 10.53 ms at 95 per second) on any provider a few
//     milliseconds away, and refusing them would refuse every call on a new
//     connection, close it, and never refill the pool: nothing sent,
//     nothing recorded, no outage.
//   - A stall after GotConn, or lateness the call already had when it asked
//     (a write then comes after the moment plus the dial), is not excused.
//     Unexcused, a stall's late requests, refused on their pooled
//     connections, would leave in a burst on the fresh ones that replace
//     them (a busiest second of 103 after a 250 ms stall:
//     TestDeliveryR8RateLimit, 20 runs pinned to two cores, 16 Nov 2026).
//
// What it costs the cap: a request can leave late by at most the time it
// waited for its connection.
func (u *connUse) got(info httptrace.GotConnInfo) {
	u.gotConn(info.Conn)
	wc, ok := info.Conn.(*writeMarkConn)
	if !ok || u.leaveBy.IsZero() {
		return
	}
	u.mu.Lock()
	wait := time.Duration(0)
	if !u.getAt.IsZero() {
		wait = time.Since(u.getAt)
	}
	u.mu.Unlock()
	wc.late.Store(&lateCheck{by: u.leaveBy.Add(wait), refused: &u.refused})
}

func (u *connUse) gotConn(c net.Conn) {
	u.mu.Lock()
	defer u.mu.Unlock()
	wc, ok := c.(*writeMarkConn)
	if !ok {
		u.other = true
		return
	}
	u.conns = append(u.conns, usedConn{conn: wc, mark: wc.writes.Load()})
}

// notSent reports whether the failed call obtained a connection and wrote
// nothing to any connection it obtained. It closes them first, so no write
// can start after the answer: a write already started counts as sent. No
// connection obtained is not "not sent": the dial failed, a provider fault
// to record (ErrNotSent).
func (u *connUse) notSent() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.conns) == 0 || u.other {
		return false
	}
	for _, c := range u.conns {
		_ = c.conn.Close() // the call failed: the transport drops it anyway
	}
	for _, c := range u.conns {
		if c.conn.writes.Load() != c.mark {
			return false
		}
	}
	return true
}
