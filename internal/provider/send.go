// Package provider calls the external notification provider (R16, decisions
// 0006, 0007, 0019) and is the one place that decides what its answer means
// (Classify).
package provider

import (
	"bytes"
	"context"
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

// ErrNotSent wraps a failed call of which no byte left this process: no
// connection was obtained for it, or none was written to before the call
// ended (a timeout, the caller's deadline, a refused dial). The provider
// cannot have seen it, so it is no attempt (decision 0005): the caller
// gives the job back instead of recording a result. Measured on the
// two-core acceptance runs: a descheduled worker's 500 ms calls ended after
// 710-946 ms with nothing written, and the double counted no request.
var ErrNotSent = errors.New("the request never left")

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
	// Every connection reports whether a write was ever started on it, so a
	// failed call can tell "nothing left" from "may have left" (ErrNotSent).
	dial := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &writeMarkConn{Conn: c}, nil
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
	req = req.WithContext(httptrace.WithClientTrace(cctx, &httptrace.ClientTrace{GotConn: used.got}))

	res, err := c.http.Do(req)
	if err != nil {
		if errors.Is(context.Cause(cctx), ErrTimeout) && ctx.Err() == nil {
			err = fmt.Errorf("%w after %s", ErrTimeout, c.timeout)
		}
		if !used.mayHaveLeft() {
			err = fmt.Errorf("%w: %w", ErrNotSent, err)
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
// not move while a request held the connection means none of it was written.
// HTTP/2 shares a connection between calls, so another call's writes can move
// it: the answer then errs toward "may have left", never the other way.
type writeMarkConn struct {
	net.Conn
	writes atomic.Int64
}

// Write counts the write before making it: a write that has started may
// have sent bytes, whether or not it returns.
func (c *writeMarkConn) Write(b []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(b)
}

// connUse is what one call learned about its connection (httptrace.GotConn).
type connUse struct {
	mu       sync.Mutex
	conn     *writeMarkConn
	mark     int64 // conn.writes when the call got it
	obtained bool  // GotConn fired
}

func (u *connUse) got(info httptrace.GotConnInfo) { u.gotConn(info.Conn) }

func (u *connUse) gotConn(c net.Conn) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.obtained = true
	if tc, ok := c.(interface{ NetConn() net.Conn }); ok { // TLS: count the writes underneath
		c = tc.NetConn()
	}
	if wc, ok := c.(*writeMarkConn); ok {
		u.conn, u.mark = wc, wc.writes.Load()
	}
}

// mayHaveLeft reports whether any byte of the failed call can have left.
// It closes the call's connection first, so no write can start after the
// answer: a write already started counts as sent. A connection not dialled
// by this client (none should exist) counts as sent.
func (u *connUse) mayHaveLeft() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	switch {
	case !u.obtained:
		return false // no connection was obtained: nothing was written
	case u.conn == nil:
		return true
	}
	_ = u.conn.Close() // the call failed: the transport drops it anyway
	return u.conn.writes.Load() != u.mark
}
