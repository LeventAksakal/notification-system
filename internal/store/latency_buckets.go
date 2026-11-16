package store

import (
	"math/bits"
	"slices"
)

// latencySubBucketBits is how many leading bits of a latency in ms its bucket
// keeps (log-linear, as HDR histograms do): every millisecond up to 16 ms has
// its own bucket, and above that each power-of-two range splits into 8 equal
// buckets, so a bucket is at most 1/8 of its lower bound wide.
//
// Why 4 (12.5%): at each latency the system itself adds more noise than a
// bucket is wide. Below 16 ms buckets are exact (the wake-hint target of a
// 30 ms p50 is read to 2 ms). From 128 to 512 ms, where the 200 ms send
// alignment and the 50 to 250 ms idle poll put a normal send, buckets are 16
// to 32 ms, under the 50 ms poll step. At the 5 s provider timeout they are
// 512 ms; retry delays are drawn from random(0, base*2^n), 100% jitter. One
// bit more halves the error and doubles the rows a busy minute writes; one
// less doubles the error. Tune it against the accuracy test in
// latency_buckets_test.go and the rollup row count per minute. The same
// number is in the bucket expression of the rollup triggers in schema/; a
// test asserts they agree.
const latencySubBucketBits = 4

// latencyBucket returns the upper bound, in ms and inclusive, of the bucket
// that holds ms. Buckets are (lower, upper]; 0 and below share bucket 0.
func latencyBucket(ms int64) int64 {
	if ms <= 0 {
		return 0
	}
	shift := latencyShift(ms - 1)
	return ((ms-1)>>shift + 1) << shift
}

// latencyBucketLower is the exclusive lower bound of the bucket whose upper
// bound is le (from latencyBucket).
func latencyBucketLower(le int64) int64 {
	if le <= 0 {
		return -1 // bucket 0 holds exactly 0
	}
	return le - 1<<latencyShift(le-1)
}

// latencyShift is log2 of the bucket width for values v+1: every value whose
// v has the same bit length shares one width.
func latencyShift(v int64) uint {
	return uint(max(bits.Len64(uint64(v))-latencySubBucketBits, 0)) //nolint:gosec // v >= 0
}

// latencyHistogram counts sent notifications by latency bucket (upper bound
// in ms -> count).
type latencyHistogram map[int64]int64

func (h latencyHistogram) add(other latencyHistogram) {
	for le, n := range other {
		h[le] += n
	}
}

// percentiles estimates p50, p95 and p99 from the buckets: the nearest rank
// ceil(p*n) is located in its bucket, and the value is interpolated linearly
// inside it, as Prometheus' histogram_quantile does. The estimate lies in the
// same bucket as the exact nearest-rank value, so it is off by less than the
// bucket width: exact up to 16 ms, under 12.5% above.
func (h latencyHistogram) percentiles() Latency {
	les := make([]int64, 0, len(h))
	var n int64
	for le, c := range h {
		if c > 0 {
			les = append(les, le)
			n += c
		}
	}
	if n == 0 {
		return Latency{}
	}
	slices.Sort(les)
	at := func(pct int64) int {
		rank := nearestRank(pct, n)
		var below int64
		for _, le := range les {
			c := h[le]
			if below+c >= rank {
				lower := max(latencyBucketLower(le), 0)
				if le == 0 {
					return 0
				}
				// ceil(lower + width*(rank-below)/c) in integers: lies in (lower, le].
				width := le - lower
				return int(lower + (width*(rank-below)+c-1)/c)
			}
			below += c
		}
		return int(les[len(les)-1])
	}
	return Latency{P50: at(50), P95: at(95), P99: at(99)}
}

// nearestRank is ceil(pct/100 * n) in integers, without float rounding.
func nearestRank(pct, n int64) int64 {
	return (pct*n + 99) / 100
}
