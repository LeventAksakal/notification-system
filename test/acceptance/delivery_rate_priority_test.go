//go:build integration

package acceptance

import (
	"fmt"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// dlMinRate is the least average rate per channel a saturated drain of one
// priority must reach: the paced rate less 2/s (decision 0009 named 95 when
// the limiter paced at 97; with the default headroom of 5 % it paces at
// dlPacedRate = 95, so the floor is 93).
const dlMinRate = dlPacedRate - 2

// R8: Implement rate limiting: maximum 100 messages per second per channel
// (with 3 workers and a backlog, the provider never sees more than 100 per
// channel in any second; decision 0009: and the limit is used).
// Dials: default APP_RATE_LIMIT_PER_SECOND=100, APP_RATE_LIMIT_HEADROOM_PERCENT=5.
func TestDeliveryR8RateLimit(t *testing.T) {
	const perChannel = 700
	chans := []string{"sms", "email", "push"}
	s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(nil)})
	s.dlCreateMany(t, perChannel*3, func(i int) apiclient.NotificationCreate {
		return msg(chans[i%3], fmt.Sprintf("R8 backlog %d", i))
	})
	// The backlog is queued before any worker starts, so the drain is steady
	// from its first request to its last.
	for range 3 {
		s.StartWorker()
	}
	s.dlAllSent(t, 45*time.Second, perChannel*3, "1=1")

	// The limiter paces at dlPacedRate (95/s; a quiet machine measured 96-97
	// when it paced at 97). The floors below are the paced rate less a margin
	// so a loaded CI runner does not fail a correct build. The measured
	// numbers are logged here and reported in docs/REQUIREMENTS.md.
	// The cap (r8MaxPerSecond) is unconditional. Only the two throughput
	// floors give way to ACCEPTANCE_RATE_FLOOR on a starved runner, where the
	// worker gives up reserved slots on slow claims and late timers and the
	// rate sags (measured 56-61/s on 2 cores with correct code).
	const (
		r8MaxPerSecond   = 100
		r8EdgeTrimLength = time.Second
	)
	r8MinWholeRate := dlRateFloor(t, "R8 whole drain", dlPacedRate-7)   // 88/s: the limit is used, not just respected
	r8MinSteadyRate := dlRateFloor(t, "R8 steady state", dlPacedRate-4) // 91/s: the drain without its first and last second
	st := s.Provider.Stats()
	for _, ch := range chans {
		cs := st.Channels[ch]
		rs := s.dlRequests(fakeprovider.Filter{Channel: ch})
		n, span, rate := dlRate(rs)
		t.Logf("%s: %d requests over %s = %.2f/s average; busiest 1 s window %d requests", ch, n, span.Round(time.Millisecond), rate, cs.MaxRequestsPerSecond)
		t.Logf("%s: requests per 1 s bucket from the first request: %v", ch, dlR8Histogram(rs))
		steady := dlR8Trim(rs, r8EdgeTrimLength)
		sn, sspan, srate := dlRate(steady)
		t.Logf("%s: steady state (first and last %s dropped): %d requests over %s = %.2f/s", ch, r8EdgeTrimLength, sn, sspan.Round(time.Millisecond), srate)
		if gi, gap := dlR8LargestGap(rs); gi > 0 {
			t.Logf("%s: largest gap %s between request %d and %d, at +%s of %s", ch, gap.Round(time.Microsecond), gi-1, gi, rs[gi].ReceivedAt.Sub(rs[0].ReceivedAt).Round(time.Millisecond), span.Round(time.Millisecond))
		}
		if cs.MaxRequestsPerSecond > r8MaxPerSecond {
			t.Errorf("%s: %d requests in one second at the provider, limit %d", ch, cs.MaxRequestsPerSecond, r8MaxPerSecond)
		}
		if rate < r8MinWholeRate {
			t.Errorf("%s: average %.2f requests/s over the whole drain, want at least %.2f (the limit must be used)", ch, rate, r8MinWholeRate)
		}
		if srate < r8MinSteadyRate {
			t.Errorf("%s: steady-state average %.2f requests/s, want at least %.2f (the limit must be used)", ch, srate, r8MinSteadyRate)
		}
	}
}

// dlR8Histogram counts the requests in each 1 s bucket from the first one.
// rs is sorted by arrival.
func dlR8Histogram(rs []fakeprovider.Request) []int {
	if len(rs) == 0 {
		return nil
	}
	var out []int
	for _, r := range rs {
		b := int(r.ReceivedAt.Sub(rs[0].ReceivedAt) / time.Second)
		for len(out) <= b {
			out = append(out, 0)
		}
		out[b]++
	}
	return out
}

// dlR8Trim keeps the requests that arrived at least edge after the first and
// at least edge before the last. rs is sorted by arrival.
func dlR8Trim(rs []fakeprovider.Request, edge time.Duration) []fakeprovider.Request {
	if len(rs) == 0 {
		return nil
	}
	from, to := rs[0].ReceivedAt.Add(edge), rs[len(rs)-1].ReceivedAt.Add(-edge)
	var out []fakeprovider.Request
	for _, r := range rs {
		if !r.ReceivedAt.Before(from) && !r.ReceivedAt.After(to) {
			out = append(out, r)
		}
	}
	return out
}

// dlR8LargestGap returns the index i of the request ending the largest gap
// between consecutive arrivals (rs[i-1] to rs[i]) and the gap; i is 0 when
// there are fewer than two requests. rs is sorted by arrival.
func dlR8LargestGap(rs []fakeprovider.Request) (int, time.Duration) {
	at, gap := 0, time.Duration(0)
	for i := 1; i < len(rs); i++ {
		if g := rs[i].ReceivedAt.Sub(rs[i-1].ReceivedAt); at == 0 || g > gap {
			at, gap = i, g
		}
	}
	return at, gap
}

// R9: Priority queue support (high, normal, low)
// (a high message queued behind a large low backlog is sent within a bounded
// time; low traffic still moves under sustained high traffic; decision 0010).
// Dials: default APP_PRIORITY_WEIGHTS=70,20,10, APP_RATE_LIMIT_PER_SECOND=100.
func TestDeliveryR9Priority(t *testing.T) {
	t.Run("a high notification behind 5000 low is sent within 2 s", func(t *testing.T) {
		s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(nil)})
		s.dlCreateMany(t, 5000, func(i int) apiclient.NotificationCreate {
			b := msg("sms", fmt.Sprintf("R9 low %d", i))
			b.Priority = ptr(apiclient.Low)
			return b
		})
		for range 3 {
			s.StartWorker()
		}
		dlEventually(t, 15*time.Second, "the low backlog is draining (100 sms requests)", func() (bool, string) {
			n := s.Provider.Stats().Channels["sms"].Received
			return n >= 100, fmt.Sprintf("%d sms requests", n)
		})
		hb := msg("sms", "R9 high behind the backlog")
		hb.Priority = ptr(apiclient.High)
		created := time.Now()
		high := s.create(t, hb)
		dlEventually(t, 8*time.Second, "the high notification is sent", func() (bool, string) {
			st := s.storedStatus(t, high.Id)
			return st == "sent", "stored status " + st
		})
		got := s.get(t, high.Id)
		if got.FinishedAt == nil {
			t.Fatalf("sent notification has no finishedAt")
		}
		// Both times come from the database clock.
		if lag := got.FinishedAt.Sub(got.CreatedAt); lag > 2*time.Second {
			t.Errorf("high notification sent %s after its creation, want within 2s", lag)
		}
		rs := s.dlRequests(fakeprovider.Filter{Key: high.Id.String()})
		if len(rs) == 0 {
			t.Fatal("the double never received the high notification")
		}
		lag := rs[0].ReceivedAt.Sub(created)
		t.Logf("high notification reached the provider %s after POST; sms backlog still %v", lag.Round(time.Millisecond), s.dlCounts(t, "priority = 'low'"))
		if lag > 2*time.Second {
			t.Errorf("high notification reached the provider %s after POST, want within 2s", lag)
		}
	})

	t.Run("low still moves while high saturates the channel", func(t *testing.T) {
		const highN, lowN = 1500, 300
		s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(nil)})
		ids := s.dlCreateMany(t, highN+lowN, func(i int) apiclient.NotificationCreate {
			b := msg("email", fmt.Sprintf("R9 mixed %d", i))
			if i < highN {
				b.Priority = ptr(apiclient.High)
			} else {
				b.Priority = ptr(apiclient.Low)
			}
			return b
		})
		isLow := map[string]bool{}
		for _, id := range ids[highN:] {
			isLow[id.String()] = true
		}
		for range 3 {
			s.StartWorker()
		}
		var first time.Time
		dlEventually(t, 15*time.Second, "the drain starts", func() (bool, string) {
			rs := s.dlRequests(fakeprovider.Filter{Channel: "email"})
			if len(rs) == 0 {
				return false, "no email request"
			}
			first = rs[0].ReceivedAt
			return true, ""
		})
		window := 8 * time.Second
		dlEventually(t, 30*time.Second, "8 s of draining pass", func() (bool, string) {
			rs := s.dlRequests(fakeprovider.Filter{Channel: "email"})
			return len(rs) > 0 && !rs[len(rs)-1].ReceivedAt.Before(first.Add(window)), fmt.Sprintf("%d email requests", len(rs))
		})
		low, all := 0, 0
		for _, r := range s.dlRequests(fakeprovider.Filter{Channel: "email"}) {
			if r.ReceivedAt.Before(first.Add(window)) && r.Delivery {
				all++
				if isLow[r.IdempotencyKey] {
					low++
				}
			}
		}
		share := 0.0
		if all > 0 {
			share = float64(low) / float64(all)
		}
		t.Logf("first %s of the drain: %d sent, %d of them low (%.1f%%)", window, all, low, 100*share)
		// Low's weight is 10 of 100; half of that share is the floor.
		if all == 0 || share < 0.05 {
			t.Errorf("low starved: %d of %d sent in %s were low (%.1f%%), want at least 5%%", low, all, window, 100*share)
		}
	})

	t.Run("low alone uses the whole channel rate", func(t *testing.T) {
		const lowN = 400
		s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(nil)})
		s.dlCreateMany(t, lowN, func(i int) apiclient.NotificationCreate {
			b := msg("push", fmt.Sprintf("R9 low alone %d", i))
			b.Priority = ptr(apiclient.Low)
			return b
		})
		for range 3 {
			s.StartWorker()
		}
		s.dlAllSent(t, 30*time.Second, lowN, "1=1")
		n, span, rate := dlRate(s.dlRequests(fakeprovider.Filter{Channel: "push"}))
		t.Logf("low alone: %d requests over %s = %.2f/s", n, span.Round(time.Millisecond), rate)
		minRate := dlRateFloor(t, "R9 low alone", dlMinRate)
		if rate < minRate {
			t.Errorf("low alone drained at %.2f/s, want at least %.2f (unused shares are lent to low)", rate, minRate)
		}
	})
}
