//go:build integration

package acceptance

import (
	"testing"
	"time"
)

// B5: Bonus: Distributed Tracing - "One trace links the API request, the queue
// wait and the provider call" (decision 0019; the coordinator's brief: the
// worker continues the stored trace, so the provider sees the same trace id).
func TestB5TraceparentToProvider(t *testing.T) {
	s := startStack(t, stackOptions{Workers: 1})

	t.Run("incoming trace id reaches the provider with a new span", func(t *testing.T) {
		traceID, spanID := randHex(32), randHex(16)
		n := s.create(t, msg("push", "b5 traced"), "traceparent", "00-"+traceID+"-"+spanID+"-01")
		reqs := s.providerRequests(t, n.ID, 60*time.Second)
		for _, r := range reqs {
			gotTrace, gotSpan, ok := parseTraceparent(r.Traceparent)
			if !ok {
				t.Errorf("provider traceparent is not valid W3C: %q", r.Traceparent)
				continue
			}
			if gotTrace != traceID {
				t.Errorf("provider trace id: want %s (the request's), got %s", traceID, gotTrace)
			}
			if gotSpan == spanID {
				t.Errorf("provider span id equals the API request's span %s; want the worker's own span", spanID)
			}
		}
	})

	t.Run("no incoming traceparent: provider still gets a valid one", func(t *testing.T) {
		n := s.create(t, msg("sms", "b5 untraced"))
		reqs := s.providerRequests(t, n.ID, 60*time.Second)
		for _, r := range reqs {
			if _, _, ok := parseTraceparent(r.Traceparent); !ok {
				t.Errorf("provider traceparent is not valid W3C: %q", r.Traceparent)
			}
		}
	})
}
