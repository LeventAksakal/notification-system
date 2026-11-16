//go:build integration

package acceptance

import (
	"fmt"
	"math"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

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
		if _, ok := dlWaitProviderRequest(s, n.Id.String(), 30*time.Second); !ok {
			t.Fatalf("warm-up %s: %s not delivered within 30 s", ch, n.Id)
		}
	}

	lat := make([]time.Duration, 0, samples)
	for i := range samples {
		dlWaitUntil(time.Now().Add(quiet))
		ch := chans[i%channels]
		r, err := s.API.CreateNotificationWithResponse(t.Context(), nil, msg(ch, fmt.Sprintf("R7 wake sample %d", i)))
		created := time.Now()
		if err != nil {
			t.Fatalf("sample %d: %v", i, err)
		}
		if r.StatusCode() != http.StatusCreated || r.JSON201 == nil || r.JSON201.Id == uuid.Nil {
			t.Fatalf("sample %d: want 201 with an id, got %s", i, show(r))
		}
		id := r.JSON201.Id
		req, ok := dlWaitProviderRequest(s, id.String(), perSend)
		if !ok {
			t.Fatalf("sample %d (%s): %s not delivered within %s", i, ch, id, perSend)
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

// R7: Process notifications asynchronously via queue workers ("POST returns
// before delivery; workers deliver"), the scheduled-tail case of the
// wake-hint work (adaptive send alignment, record 0009): a row posted right
// after a lone send on an idle channel reaches the provider within 100 ms of
// its POST (p99 over 30 pairs), where before the give-back it waited behind
// the first group's unused reservation (about 200 ms plus the alignment).
func TestGapDeliveryR7SecondRowAfterALoneSend(t *testing.T) {
	const (
		pairs   = 30
		quiet   = 2 * time.Second
		maxP99  = 100 * time.Millisecond
		perSend = 5 * time.Second
	)
	s := startStack(t, stackOptions{Workers: 3})
	chans := []string{"sms", "email", "push"}
	post := func(t *testing.T, ch, content string) (uuid.UUID, time.Time) {
		t.Helper()
		r, err := s.API.CreateNotificationWithResponse(t.Context(), nil, msg(ch, content))
		created := time.Now()
		if err != nil {
			t.Fatalf("%s: %v", content, err)
		}
		if r.StatusCode() != http.StatusCreated || r.JSON201 == nil || r.JSON201.Id == uuid.Nil {
			t.Fatalf("%s: want 201 with an id, got %s", content, show(r))
		}
		return r.JSON201.Id, created
	}
	for _, ch := range chans {
		id, _ := post(t, ch, "R7 tail warm-up "+ch)
		if _, ok := dlWaitProviderRequest(s, id.String(), 30*time.Second); !ok {
			t.Fatalf("warm-up %s: %s not delivered within 30 s", ch, id)
		}
	}

	lat := make([]time.Duration, 0, pairs)
	for i := range pairs {
		dlWaitUntil(time.Now().Add(quiet))
		ch := chans[i%len(chans)]
		first, _ := post(t, ch, fmt.Sprintf("R7 tail lone %d", i))
		if _, ok := dlWaitProviderRequest(s, first.String(), perSend); !ok {
			t.Fatalf("pair %d (%s): the lone send %s not delivered within %s", i, ch, first, perSend)
		}
		second, created := post(t, ch, fmt.Sprintf("R7 tail second %d", i))
		req, ok := dlWaitProviderRequest(s, second.String(), perSend)
		if !ok {
			t.Fatalf("pair %d (%s): the second row %s not delivered within %s", i, ch, second, perSend)
		}
		lat = append(lat, req.ReceivedAt.Sub(created))
	}
	slices.Sort(lat)
	p50, p99 := dlNearestRank(lat, 50), dlNearestRank(lat, 99)
	t.Logf("second row after a lone send, 201 -> provider, %d pairs: p50 %s, p99 %s, max %s", pairs, p50, p99, lat[len(lat)-1])
	if p99 > maxP99 {
		t.Errorf("p99 %s, want <= %s", p99, maxP99)
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
