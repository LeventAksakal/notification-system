// Package delivery is the delivery loop of `app worker` (rows R7, R8, R9, R11,
// R12, B1, B2). Per channel it asks the channel's shared provider health
// (decision 0026), reserves the send slots of
// the next horizon in one limiter call, splits them between the priorities by
// weight, claims the rows with one query per priority, and sends each job at
// its slot. It classifies the answer and writes the result.
//
// Timing of one job: the slot comes from the limiter (Valkey's clock, exactly
// one interval apart across all workers), reserved a lead ahead that follows
// the worker's measured claim time (lead); the request leaves at its slot,
// never earlier. The slot is mapped to the local clock through the
// offset learnt from every Take's round trip (clockMap), never from the moment
// the reply arrived, so a late reply does not shift its group late. A job that
// cannot leave within one interval of that moment (slow claim, no free
// provider call, a timer late under load, the mapping's own uncertainty) is
// released unsent and claimed again for a later slot: it is never sent late on
// top of the full rate. Before the call the lease is checked: the call ends
// before the lease does, or it is not made.
package delivery

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
	"github.com/LeventAksakal/notification-system/internal/wake"
)

// Sender makes one provider call (provider.Client). It must end the call when
// ctx ends.
type Sender interface {
	Send(ctx context.Context, m provider.Message) (provider.Response, error)
}

// Config is the worker's dials.
type Config struct {
	// Concurrency is APP_WORKER_CONCURRENCY: provider calls in flight per
	// process. Each channel has places of its own (Concurrency/(2*channels),
	// at least 1) and shares the rest with the others, so a channel whose
	// provider hangs can hold at most its own places and the shared ones,
	// never another channel's.
	Concurrency     int
	MaxAttempts     int           // APP_MAX_ATTEMPTS: counted failures before a notification is exhausted
	RetryBase       time.Duration // APP_RETRY_BASE
	RetryCap        time.Duration // APP_RETRY_CAP
	Lease           time.Duration // APP_LEASE_DURATION
	ReapInterval    time.Duration // APP_REAP_INTERVAL
	ShutdownTimeout time.Duration // APP_SHUTDOWN_TIMEOUT
	Weights         [3]int        // APP_PRIORITY_WEIGHTS
	Channels        []notification.Channel
	// Horizon is APP_RATE_LIMIT_HORIZON: the least span of slots one Take
	// reserves past the lead (see horizon). Default 200ms.
	Horizon time.Duration
	// AlignFloor and AlignCeiling bound the send alignment, the lead a
	// group's first slot is reserved with (see lead). Defaults alignFloor and
	// alignCeiling(Lease). Not operator dials: tests fix the alignment by
	// setting both to one value.
	AlignFloor, AlignCeiling time.Duration
	// IdleMin and IdleMax bound the retry waits of a channel that cannot
	// reserve now: IdleMin while all its provider calls are in flight (a call
	// place frees within a call's time), and the clock probe's back-off from
	// IdleMin doubling to IdleMax; IdleMax after the limiter or a claim failed
	// and as the cap of a refused health check's wait. Defaults 50ms and 250ms:
	// at full load a call place frees every 500 ms / 72 = 7 ms (assumed
	// provider p99 over APP_WORKER_CONCURRENCY), so 50 ms wakes once several
	// have freed and gives up at most about five of a channel's 10.5 ms slots
	// to the other workers; 250 ms keeps a failing limiter or database at 4
	// retries a second per channel per worker (36 for 3 workers x 3 channels)
	// while bounding the first send after the fault clears. Both are free
	// choices within those bounds; tune by the slots given up when
	// concurrency binds. A channel
	// with nothing due does not poll: it sleeps until its next due time, a
	// wake hint, or SafetyPoll.
	IdleMin, IdleMax time.Duration
	// SafetyPoll is the longest an empty priority sleeps without asking the
	// database (Queue.NextDue): it bounds the delay a lost wake hint can
	// cause. Default wake.SafetyPoll; not an operator dial (tests shorten it).
	SafetyPoll time.Duration
	// ProviderTimeout is APP_PROVIDER_TIMEOUT, the Sender's own limit on one
	// call; it bounds the probe lease (probeLease). 0: the lease bounds it.
	ProviderTimeout time.Duration
	// HealthCooldown is APP_BREAKER_COOLDOWN. Rows whose failure belongs to an
	// outage are spread over one cool-down after the next probe may go, so
	// they do not come due together and declare the outage again by
	// themselves (decision 0026). 0: no spread.
	HealthCooldown time.Duration
}

// Deps are the worker's collaborators.
type Deps struct {
	Queue     notification.Queue
	Limiter   notification.Limiter
	Sender    Sender
	Publisher notification.Publisher
	// Health is the channels' provider health shared by every worker
	// (health.ValkeyState in production); nil: a state that never opens.
	Health health.State
	Tracer trace.Tracer
	// Rand returns a uniform value in [0, n]; default math/rand/v2.
	Rand func(n int64) int64
	// Now is the clock a job's lease is checked against; default time.Now.
	Now func() time.Time
	// Meter provides the delivery metrics; default the global provider.
	Meter metric.MeterProvider
	// Waker tells the other workers (and this one) that a job went back to
	// pending: released, rescheduled or reaped. Optional.
	Waker notification.Waker
	// Wakes delivers the hints published for the worker's channels
	// (wake.Listener). Optional: without it an empty priority is asked again
	// after its due time or SafetyPoll only.
	Wakes Wakes
}

// Wakes is a source of wake hints (wake.Listener). Run holds a subscription
// until ctx ends, calling onHint for each hint and onSubscribed after every
// (re)subscribe; both must not block.
type Wakes interface {
	Run(ctx context.Context, channels []notification.Channel, onHint func(notification.Wake), onSubscribed func()) error
}

// Worker runs the delivery loop; Run once.
type Worker struct {
	cfg   Config
	deps  Deps
	gates map[notification.Channel]*health.Gate
	clock *clockMap // Valkey's clock to the local one, for every channel
	m     metrics
	// runs is set by Run before anything that reads it starts.
	runs map[notification.Channel]*channelRun
}

// New returns a worker; zero dials get their defaults.
func New(cfg Config, deps Deps) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	if cfg.IdleMin <= 0 {
		cfg.IdleMin = 50 * time.Millisecond
	}
	if cfg.Horizon <= 0 {
		cfg.Horizon = 200 * time.Millisecond
	}
	if cfg.AlignFloor <= 0 {
		cfg.AlignFloor = alignFloor
	}
	if cfg.AlignCeiling <= 0 {
		cfg.AlignCeiling = alignCeiling(cfg.Lease)
	}
	cfg.AlignCeiling = max(cfg.AlignCeiling, cfg.AlignFloor)
	if cfg.IdleMax < cfg.IdleMin {
		cfg.IdleMax = max(250*time.Millisecond, cfg.IdleMin)
	}
	if len(cfg.Channels) == 0 {
		cfg.Channels = notification.Channels
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = 5 * time.Second
	}
	if cfg.SafetyPoll <= 0 {
		cfg.SafetyPoll = wake.SafetyPoll
	}
	if deps.Rand == nil {
		deps.Rand = func(n int64) int64 { return rand.Int64N(n + 1) } //nolint:gosec // jitter, not security
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Tracer == nil {
		deps.Tracer = noop.NewTracerProvider().Tracer("worker")
	}
	if deps.Health == nil {
		deps.Health = health.NeverOpens()
	}
	gates := make(map[notification.Channel]*health.Gate, len(cfg.Channels))
	for _, ch := range cfg.Channels {
		gates[ch] = health.NewGate(deps.Health, ch)
	}
	// A placed slot waits at most the lead plus the horizon, both at their largest.
	span := time.Second + cfg.AlignCeiling + max(cfg.Horizon, cfg.AlignCeiling)
	return &Worker{cfg: cfg, deps: deps, gates: gates, clock: newClockMap(time.Now(), clockWindow, span), m: newMetrics(deps.Meter)}
}

// String is for logs of a worker's configuration.
func (c Config) String() string {
	return fmt.Sprintf("concurrency=%d max_attempts=%d weights=%v horizon=%s send_align=[%s,%s]",
		c.Concurrency, c.MaxAttempts, c.Weights, c.Horizon, c.AlignFloor, c.AlignCeiling)
}
