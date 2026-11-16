//go:build integration

package intake_test

import (
	"flag"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/support"
)

// measure turns on TestMeasureIntakeLatency; it takes about a minute and only
// logs numbers (decision 0016 "As built", the cost of the one clock):
//
//	go test -tags integration ./internal/intake -run TestMeasureIntakeLatency -measure -v
var measure = flag.Bool("measure", false, "run the intake latency measurement (about a minute)")

// Sample sizes of the measurement, arbitrary: enough samples that p99 is not
// the single worst one (2000 creates, 60 batches), few enough to finish in
// about a minute on a laptop. Warm-up runs fill the connection pool and the
// InnoDB buffer pool first and are not counted.
const (
	measureCreates = 2000
	measureBatches = 60
	measureWarmup  = 10
)

// TestMeasureIntakeLatency logs p50 and p99 of Create and of a full
// CreateBatch, each with and without scheduledAt, sequentially from one
// client against one MySQL container, so the before and after runs of a
// change see the same load.
func TestMeasureIntakeLatency(t *testing.T) {
	if !*measure {
		t.Skip("pass -measure to run")
	}
	db, _ := support.MySQL(t)
	svc := intake.New(db, nopPublisher{}, time.Hour)
	ctx := t.Context()
	content := "measure"
	req := func(scheduled bool) intake.Request {
		r := intake.Request{To: "+905551234567", Channel: notification.ChannelSMS, Content: &content}
		if scheduled {
			at := time.Now().Add(time.Hour)
			r.ScheduledAt = &at
		}
		return r
	}

	// The one round trip the database clock adds to a scheduled request,
	// measured alone so it can be told apart from the noise of the commits.
	var reads []time.Duration
	for i := range measureCreates + measureWarmup {
		var now time.Time
		st := time.Now()
		if err := db.QueryRowContext(ctx, "SELECT CAST(NOW(3) AS DATETIME(3))").Scan(&now); err != nil {
			t.Fatal(err)
		}
		if i >= measureWarmup {
			reads = append(reads, time.Since(st))
		}
	}
	t.Logf("clock read      %s", pct(reads))

	for _, scheduled := range []bool{false, true} {
		name := "unscheduled"
		if scheduled {
			name = "scheduled"
		}
		var creates []time.Duration
		for i := range measureCreates + measureWarmup {
			r := req(scheduled)
			st := time.Now()
			if _, _, err := svc.Create(ctx, r, intake.Meta{CorrelationID: "m" + strconv.Itoa(i)}); err != nil {
				t.Fatal(err)
			}
			if i >= measureWarmup {
				creates = append(creates, time.Since(st))
			}
		}
		t.Logf("create %-11s %s", name, pct(creates))

		var batches []time.Duration
		for i := range measureBatches + measureWarmup {
			rs := make([]intake.Request, notification.MaxBatchSize)
			for j := range rs {
				rs[j] = req(scheduled)
			}
			st := time.Now()
			if _, _, err := svc.CreateBatch(ctx, rs, intake.Meta{CorrelationID: "b" + strconv.Itoa(i)}); err != nil {
				t.Fatal(err)
			}
			if i >= measureWarmup {
				batches = append(batches, time.Since(st))
			}
		}
		t.Logf("batch  %-11s %s (%d items)", name, pct(batches), notification.MaxBatchSize)
	}
}

// pct formats the count, p50 and p99 of ds.
func pct(ds []time.Duration) string {
	s := slices.Clone(ds)
	slices.Sort(s)
	at := func(p float64) time.Duration { return s[min(len(s)-1, int(float64(len(s))*p))] }
	return fmt.Sprintf("n=%d p50=%s p99=%s", len(s), at(0.50).Round(10*time.Microsecond), at(0.99).Round(10*time.Microsecond))
}
