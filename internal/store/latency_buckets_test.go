package store

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestLatencyBucketsTileTheLine(t *testing.T) {
	// Every value lands in a bucket (lower, upper] that contains it, buckets
	// are contiguous, and above 16 ms none is wider than 1/8 of its lower bound.
	prev := int64(0)
	for v := int64(1); v <= 1<<22; v++ {
		le := latencyBucket(v)
		lower := latencyBucketLower(le)
		if v <= lower || v > le {
			t.Fatalf("value %d in bucket (%d, %d]", v, lower, le)
		}
		if le != prev {
			if lower != prev {
				t.Fatalf("bucket (%d, %d] does not start where (.., %d] ended", lower, le, prev)
			}
			if v > 16 && (le-lower)*8 > lower {
				t.Fatalf("bucket (%d, %d] is wider than 1/8 of its lower bound", lower, le)
			}
			prev = le
		}
	}
	if latencyBucket(0) != 0 || latencyBucket(-3) != 0 || latencyBucket(16) != 16 || latencyBucket(17) != 18 {
		t.Fatal("bucket edges moved")
	}
}

// exactPercentiles is nearest rank over the raw values: what the endpoint
// computed before rollups.
func exactPercentiles(values []int64) Latency {
	s := slices.Clone(values)
	slices.Sort(s)
	at := func(p int64) int { return int(s[nearestRank(p, int64(len(s)))-1]) }
	return Latency{P50: at(50), P95: at(95), P99: at(99)}
}

func TestBucketPercentilesStayWithinTheBucketError(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // reproducible test data
	draw := map[string]func() int64{
		// Normal sends: 200 ms alignment plus a 0..250 ms poll plus a 5..60 ms call.
		"aligned sends": func() int64 { return 200 + rng.Int64N(250) + 5 + rng.Int64N(55) },
		// Wake-hint fast path: a few ms.
		"fast path": func() int64 { return 1 + rng.Int64N(30) },
		// Heavy tail: retries after random(0, 1s*2^n) up to the 5 min cap.
		"retry tail": func() int64 {
			if rng.IntN(100) < 3 {
				return rng.Int64N(min(int64(1000)<<rng.IntN(9), 300_000))
			}
			return 300 + rng.Int64N(100)
		},
		// Backlog: log-uniform from 1 ms to an hour.
		"log uniform": func() int64 { return int64(math.Exp(rng.Float64() * math.Log(3_600_000))) },
		"one value":   func() int64 { return 431 },
	}
	for name, f := range draw {
		for _, n := range []int{1, 2, 7, 100, 10_000, 200_000} {
			values := make([]int64, n)
			h := latencyHistogram{}
			for i := range values {
				values[i] = f()
				h[latencyBucket(values[i])]++
			}
			exact, est := exactPercentiles(values), h.percentiles()
			for _, pair := range [][2]int{{exact.P50, est.P50}, {exact.P95, est.P95}, {exact.P99, est.P99}} {
				ex, got := int64(pair[0]), int64(pair[1])
				le := latencyBucket(ex)
				bound := le - latencyBucketLower(le) - 1 // both lie in (lower, le]
				if d := got - ex; d > bound || -d > bound {
					t.Errorf("%s n=%d: estimate %d, exact %d, allowed error %d", name, n, got, ex, bound)
				}
				if ex > 16 && (got-ex)*8 > ex || (ex-got)*8 > ex {
					t.Errorf("%s n=%d: estimate %d is more than 12.5%% from exact %d", name, n, got, ex)
				}
			}
		}
	}
}
