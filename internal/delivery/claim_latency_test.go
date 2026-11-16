package delivery

import (
	"testing"
	"time"
)

func TestClaimLatencyLead(t *testing.T) {
	const ms = time.Millisecond
	lo, hi := 20*ms, 3750*ms // a floor under startLead, so the rules show; alignCeiling(30 s)
	// claims of 10, 11, ... ms.
	rising := func(n int) []time.Duration {
		var cs []time.Duration
		for i := range n {
			cs = append(cs, time.Duration(10+i)*ms)
		}
		return cs
	}
	join := func(parts ...[]time.Duration) []time.Duration {
		var cs []time.Duration
		for _, p := range parts {
			cs = append(cs, p...)
		}
		return cs
	}
	many := func(n int, d time.Duration) []time.Duration {
		var cs []time.Duration
		for range n {
			cs = append(cs, d)
		}
		return cs
	}
	cases := []struct {
		name   string
		claims []time.Duration
		want   time.Duration
	}{
		{"no claim measured yet: the start-up lead", nil, startLead},
		{"one fast claim is not believed alone", []time.Duration{15 * ms}, startLead},
		{"one slow first claim is believed at once", []time.Duration{1600 * ms}, 1600 * ms},
		{"until twenty are measured, the largest", []time.Duration{15 * ms, 170 * ms, 30 * ms}, 170 * ms},
		{"twenty claims: the second largest, their p95", rising(20), 28 * ms},
		{"only the newest twenty count", join(many(5, 900*ms), rising(20)), 28 * ms},
		{"one slow outlier is ignored", join(rising(20), []time.Duration{900 * ms}), 29 * ms},
		{"a second slow claim lifts the lead at once", join(rising(20), []time.Duration{900 * ms, 1600 * ms}), 900 * ms},
		{"a slow spell holds the lead while it lasts", join(rising(20), many(5, 1600*ms), rising(3)), 1600 * ms},
		{"a slow spell is forgotten after nineteen fast claims", join(many(20, 1600*ms), many(19, 30*ms)), 30 * ms},
		{"the floor holds under very fast claims", many(20, 2*ms), lo},
		{"the ceiling holds under claims slower than the lease allows", []time.Duration{20 * time.Second, 30 * time.Second}, hi},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var cl claimLatency
			for _, d := range c.claims {
				cl.observe(d)
			}
			if got := cl.lead(lo, hi); got != c.want {
				t.Errorf("lead = %s, want %s", got, c.want)
			}
		})
	}
}
