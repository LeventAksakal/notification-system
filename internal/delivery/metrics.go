package delivery

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// meterName is the instrumentation scope of the worker's metrics.
const meterName = "github.com/LeventAksakal/notification-system/internal/delivery"

// giveUp is why a reserved slot was not used: a closed set, so the reason
// attribute stays low-cardinality.
type giveUp string

const (
	giveUpClaimOutlasted giveUp = "claim_outlasted_slots"
	giveUpSlotMissed     giveUp = "slot_missed"
	giveUpNoFreeCall     giveUp = "no_free_call"
	giveUpBreakerOpened  giveUp = "breaker_opened"
	giveUpLeaseTooShort  giveUp = "lease_too_short"
	giveUpClockUncertain giveUp = "clock_uncertain"
	giveUpClockStepped   giveUp = "clock_stepped"
)

// metrics are the worker's OpenTelemetry instruments. Attributes are the
// channel, the outcome and the reason only: never an id.
type metrics struct {
	attempts metric.Int64Counter
	duration metric.Float64Histogram
	givenUp  metric.Int64Counter
}

// newMetrics creates the instruments on mp (nil: the global provider, a
// no-op until obs.Setup installs one). A failed instrument is reported to the
// OTel error handler and stays a no-op.
func newMetrics(mp metric.MeterProvider) metrics {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	m := mp.Meter(meterName)
	attempts, err := m.Int64Counter("notifier.delivery.attempts",
		metric.WithUnit("{attempt}"), metric.WithDescription("Provider calls made, by channel and outcome."))
	if err != nil {
		otel.Handle(err)
	}
	duration, err := m.Float64Histogram("notifier.provider.call.duration",
		metric.WithUnit("s"), metric.WithDescription("Duration of one provider call, by channel."),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10))
	if err != nil {
		otel.Handle(err)
	}
	givenUp, err := m.Int64Counter("notifier.slots.given_up",
		metric.WithUnit("{slot}"), metric.WithDescription("Reserved send slots not used, by channel and reason."))
	if err != nil {
		otel.Handle(err)
	}
	return metrics{attempts: attempts, duration: duration, givenUp: givenUp}
}

func (m metrics) attempt(ctx context.Context, ch notification.Channel, outcome notification.AttemptOutcome, seconds float64) {
	m.attempts.Add(ctx, 1, metric.WithAttributes(attribute.String("channel", string(ch)), attribute.String("outcome", string(outcome))))
	m.duration.Record(ctx, seconds, metric.WithAttributes(attribute.String("channel", string(ch))))
}

func (m metrics) gaveUp(ctx context.Context, ch notification.Channel, why giveUp, n int) {
	if n <= 0 {
		return
	}
	m.givenUp.Add(ctx, int64(n), metric.WithAttributes(attribute.String("channel", string(ch)), attribute.String("reason", string(why))))
}
