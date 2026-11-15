//go:build integration

package acceptance

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

// R7: Process notifications asynchronously via queue workers ("POST returns
// before delivery; workers deliver"). The latency target is the owner's
// wake-hint objective (15 Nov 2026): a lone notification after a quiet period
// reaches the provider within p50 30 ms and p99 100 ms of its 201, where an
// idle worker used to poll every 50 ms doubling to 250 ms and every send
// waited a fixed 200 ms after its limiter slot.
func TestDeliveryR7FirstSendAfterIdleIsPrompt(t *testing.T) {
	// Twice the targets: the harness runs on a loaded developer machine
	// (Docker Desktop with other stacks) and on a two-core CI runner. p95, not
	// p99: with 20 samples p99 is the single worst sample.
	const (
		samples  = 20
		quiet    = 2 * time.Second // the old idle poll reached its 250 ms ceiling within ~0.4 s of quiet
		maxP50   = 60 * time.Millisecond
		maxP95   = 200 * time.Millisecond
		perSend  = 5 * time.Second
		channels = 3
	)
	s := startStack(t, stackOptions{Workers: 3})
	chans := []string{"sms", "email", "push"}

	// Warm up: worker start-up (pool warm-up, the first limiter clock
	// reading) is not what is measured.
	for _, ch := range chans {
		n := s.create(t, msg(ch, "R7 wake warm-up "+ch))
		if _, ok := dlWaitProviderRequest(s, n.ID, 30*time.Second); !ok {
			t.Fatalf("warm-up %s: %s not delivered within 30 s", ch, n.ID)
		}
	}

	lat := make([]time.Duration, 0, samples)
	for i := range samples {
		dlWaitUntil(time.Now().Add(quiet))
		ch := chans[i%channels]
		r, err := s.send(http.MethodPost, "/v1/notifications", msg(ch, fmt.Sprintf("R7 wake sample %d", i)))
		created := time.Now()
		if err != nil {
			t.Fatalf("sample %d: %v", i, err)
		}
		if r.Status != http.StatusCreated {
			t.Fatalf("sample %d: want 201, got %d %s", i, r.Status, r.Body)
		}
		var n apiNotification
		if err := json.Unmarshal(r.Body, &n); err != nil || n.ID == "" {
			t.Fatalf("sample %d: 201 without an id: %v %s", i, err, r.Body)
		}
		req, ok := dlWaitProviderRequest(s, n.ID, perSend)
		if !ok {
			t.Fatalf("sample %d (%s): %s not delivered within %s", i, ch, n.ID, perSend)
		}
		lat = append(lat, req.ReceivedAt.Sub(created))
	}
	slices.Sort(lat)
	p50, p95, p99 := dlNearestRank(lat, 50), dlNearestRank(lat, 95), dlNearestRank(lat, 99)
	t.Logf("201 -> provider after %s idle, %d samples: p50 %s, p95 %s, p99 %s, max %s",
		quiet, samples, p50, p95, p99, lat[len(lat)-1])
	if p50 > maxP50 {
		t.Errorf("p50 %s, want <= %s (target 30 ms, doubled for a loaded machine)", p50, maxP50)
	}
	if p95 > maxP95 {
		t.Errorf("p95 %s, want <= %s (target p99 100 ms, doubled for a loaded machine)", p95, maxP95)
	}
}

// dlWaitProviderRequest polls the provider double every 5 ms until it has a
// request for key, or timeout passes.
func dlWaitProviderRequest(s *stack, key string, timeout time.Duration) (fakeprovider.Request, bool) {
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if rs := s.dlRequests(fakeprovider.Filter{Key: key}); len(rs) > 0 {
			return rs[0], true
		}
		if time.Now().After(deadline) {
			return fakeprovider.Request{}, false
		}
		<-tick.C
	}
}

// dlNearestRank is the nearest-rank percentile p of sorted.
func dlNearestRank(sorted []time.Duration, p float64) time.Duration {
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	rank = max(1, min(rank, len(sorted)))
	return sorted[rank-1]
}
