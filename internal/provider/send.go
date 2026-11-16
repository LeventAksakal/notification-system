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
	"net/http"
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

	res, err := c.http.Do(req)
	if err != nil {
		if errors.Is(context.Cause(cctx), ErrTimeout) && ctx.Err() == nil {
			return Response{}, fmt.Errorf("%w after %s", ErrTimeout, c.timeout)
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
