// Package config is the one place the service reads its environment. Every
// dial has a default, bounds and a doc line; config_test.go enforces that.
// Secrets (the DSN) have no default: they are injected at runtime, never
// committed.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/LeventAksakal/notification-system/internal/core"
)

// Config is the typed, validated set of dials.
type Config struct {
	HTTPAddr          string
	DatabaseDSN       string // secret
	ValkeyAddr        string
	OTLPEndpoint      string // empty disables telemetry export
	ServiceName       string
	LogLevel          slog.Level
	DBMaxOpenConns    int
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration

	// ProviderURLs holds the provider endpoint of each channel.
	ProviderURLs    map[core.Channel]string
	ProviderTimeout time.Duration
	MaxAttempts     int
	RetryBase       time.Duration
	RetryCap        time.Duration

	RateLimitPerSecond   int
	RateLimitHeadroomPct int
	RateLimitHorizon     time.Duration
	PriorityWeights      [3]int // high, normal, low
	LeaseDuration        time.Duration
	ReapInterval         time.Duration
	WorkerConcurrency    int
	BreakerWindow        time.Duration
	BreakerMinRequests   int
	BreakerFailureRatio  float64
	BreakerCooldown      time.Duration
	IdempotencyRetention time.Duration
	IdempotencySweep     time.Duration
	WSSendBuffer         int
}

// ProviderURL returns the provider endpoint for a channel.
func (c Config) ProviderURL(ch core.Channel) string { return c.ProviderURLs[ch] }

// Dial describes one environment variable.
type Dial struct {
	Env      string
	Doc      string
	Default  string // "" with Required means the value must be injected
	Required bool
	Secret   bool
	apply    func(c *Config, raw string) error
}

// Dials lists every setting. Order is the order of the docs.
var Dials = []Dial{
	text("APP_HTTP_ADDR", ":8080", "HTTP listen address", func(c *Config) *string { return &c.HTTPAddr }),
	{Env: "APP_DATABASE_DSN", Doc: "MySQL DSN (go-sql-driver format); injected at runtime", Required: true, Secret: true,
		apply: func(c *Config, raw string) error { c.DatabaseDSN = raw; return nil }},
	text("APP_VALKEY_ADDR", "localhost:6379", "Valkey host:port", func(c *Config) *string { return &c.ValkeyAddr }),
	text("APP_OTLP_ENDPOINT", "", "OTLP/HTTP base URL, e.g. http://otel:4318; empty disables export",
		func(c *Config) *string { return &c.OTLPEndpoint }),
	text("APP_SERVICE_NAME", "notifier", "service.name on every span, metric and log", func(c *Config) *string { return &c.ServiceName }),
	{Env: "APP_LOG_LEVEL", Doc: "debug, info, warn or error", Default: "info",
		apply: func(c *Config, raw string) error { return c.LogLevel.UnmarshalText([]byte(raw)) }},
	integer("APP_DB_MAX_OPEN_CONNS", "10", "MySQL pool size", 1, 200, func(c *Config) *int { return &c.DBMaxOpenConns }),
	duration("APP_READ_HEADER_TIMEOUT", "5s", "time a client has to send request headers", time.Second, time.Minute,
		func(c *Config) *time.Duration { return &c.ReadHeaderTimeout }),
	duration("APP_SHUTDOWN_TIMEOUT", "10s", "grace period for in-flight requests on SIGTERM", time.Second, 2*time.Minute,
		func(c *Config) *time.Duration { return &c.ShutdownTimeout }),

	providerURL("APP_PROVIDER_URL_SMS", core.ChannelSMS),
	providerURL("APP_PROVIDER_URL_EMAIL", core.ChannelEmail),
	providerURL("APP_PROVIDER_URL_PUSH", core.ChannelPush),
	duration("APP_PROVIDER_TIMEOUT", "5s", "time limit of one provider call", 10*time.Millisecond, time.Minute,
		func(c *Config) *time.Duration { return &c.ProviderTimeout }),
	integer("APP_MAX_ATTEMPTS", "5", "provider calls before a notification fails as exhausted", 1, 50,
		func(c *Config) *int { return &c.MaxAttempts }),
	duration("APP_RETRY_BASE", "1s", "backoff base; attempt n waits random(0, min(cap, base*2^n))", time.Millisecond, time.Minute,
		func(c *Config) *time.Duration { return &c.RetryBase }),
	duration("APP_RETRY_CAP", "5m", "backoff cap; at least APP_RETRY_BASE", time.Millisecond, 24*time.Hour,
		func(c *Config) *time.Duration { return &c.RetryCap }),
	integer("APP_RATE_LIMIT_PER_SECOND", "100", "provider's send limit per channel, per second", 1, 100_000,
		func(c *Config) *int { return &c.RateLimitPerSecond }),
	integer("APP_RATE_LIMIT_HEADROOM_PERCENT", "3",
		"the limiter paces this many percent under the limit; send slots are exactly one interval apart, "+
			"so 0 is exact at the limiter but leaves no room for arrival jitter at the provider", 0, 50,
		func(c *Config) *int { return &c.RateLimitHeadroomPct }),
	duration("APP_RATE_LIMIT_HORIZON", "200ms", "how far ahead a worker may reserve send slots; about half of it is reserved per limiter call",
		10*time.Millisecond, 2*time.Second,
		func(c *Config) *time.Duration { return &c.RateLimitHorizon }),
	{Env: "APP_PRIORITY_WEIGHTS", Default: "70,20,10",
		Doc: "high,normal,low shares of a channel's rate; each [0..100], sum above 0; unused share is borrowed. " +
			"A weight of 0 means that priority only sends on slots the others leave unused",
		apply: func(c *Config, raw string) error { return parseWeights(raw, &c.PriorityWeights) }},
	duration("APP_LEASE_DURATION", "30s",
		"how long a claimed notification belongs to its worker; a provider call is cut 250ms before it ends", time.Second, time.Hour,
		func(c *Config) *time.Duration { return &c.LeaseDuration }),
	duration("APP_REAP_INTERVAL", "5s", "how often expired leases are returned to pending", 10*time.Millisecond, 10*time.Minute,
		func(c *Config) *time.Duration { return &c.ReapInterval }),
	integer("APP_WORKER_CONCURRENCY", "32", "provider calls in flight per worker process", 1, 1024,
		func(c *Config) *int { return &c.WorkerConcurrency }),
	duration("APP_BREAKER_WINDOW", "10s", "window the breaker's failure ratio is measured over", 100*time.Millisecond, 10*time.Minute,
		func(c *Config) *time.Duration { return &c.BreakerWindow }),
	integer("APP_BREAKER_MIN_REQUESTS", "10", "calls in the window before the failure ratio counts", 1, 10_000,
		func(c *Config) *int { return &c.BreakerMinRequests }),
	ratio("APP_BREAKER_FAILURE_RATIO", "0.5", "the breaker opens at or above this failure ratio", 0.01, 1,
		func(c *Config) *float64 { return &c.BreakerFailureRatio }),
	duration("APP_BREAKER_COOLDOWN", "5s", "open time before one half-open probe", 10*time.Millisecond, 10*time.Minute,
		func(c *Config) *time.Duration { return &c.BreakerCooldown }),
	duration("APP_IDEMPOTENCY_RETENTION", "24h", "how long an Idempotency-Key is remembered", time.Second, 30*24*time.Hour,
		func(c *Config) *time.Duration { return &c.IdempotencyRetention }),
	duration("APP_IDEMPOTENCY_SWEEP_INTERVAL", "1m", "how often app serve deletes expired Idempotency-Keys", 10*time.Millisecond, time.Hour,
		func(c *Config) *time.Duration { return &c.IdempotencySweep }),
	integer("APP_WS_SEND_BUFFER", "256", "events buffered per WebSocket subscriber before it is dropped", 1, 65_536,
		func(c *Config) *int { return &c.WSSendBuffer }),
}

// Load reads every dial through getenv (os.Getenv in main, a map in tests)
// and returns all problems at once.
func Load(getenv func(string) string) (Config, error) {
	c := Config{ProviderURLs: make(map[core.Channel]string, len(core.Channels))}
	var errs []error
	for _, d := range Dials {
		raw := getenv(d.Env)
		if raw == "" {
			raw = d.Default
		}
		if raw == "" && d.Required {
			errs = append(errs, fmt.Errorf("%s is required: %s", d.Env, d.Doc))
			continue
		}
		if err := d.apply(&c, raw); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", d.Env, err))
		}
	}
	if len(errs) == 0 && c.RetryCap < c.RetryBase {
		errs = append(errs, fmt.Errorf("APP_RETRY_CAP %s is below APP_RETRY_BASE %s", c.RetryCap, c.RetryBase))
	}
	return c, errors.Join(errs...)
}

func providerURL(env string, ch core.Channel) Dial {
	return Dial{Env: env, Default: "http://localhost:8090/send", Doc: "provider endpoint for " + string(ch) + " (http or https URL)",
		apply: func(c *Config, raw string) error {
			u, err := url.Parse(raw)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return fmt.Errorf("not an http(s) URL: %q", raw)
			}
			c.ProviderURLs[ch] = raw
			return nil
		}}
}

func ratio(env, def, doc string, lo, hi float64, field func(*Config) *float64) Dial {
	return Dial{Env: env, Default: def, Doc: fmt.Sprintf("%s [%g..%g]", doc, lo, hi),
		apply: func(c *Config, raw string) error {
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return fmt.Errorf("not a number: %q", raw)
			}
			if v < lo || v > hi {
				return fmt.Errorf("%g outside [%g..%g]", v, lo, hi)
			}
			*field(c) = v
			return nil
		}}
}

// parseWeights reads "high,normal,low": three integers in [0..100], sum above 0.
func parseWeights(raw string, out *[3]int) error {
	parts := strings.Split(raw, ",")
	if len(parts) != 3 {
		return fmt.Errorf("want three comma-separated weights (high,normal,low), got %q", raw)
	}
	var w [3]int
	sum := 0
	for i, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || v < 0 || v > 100 {
			return fmt.Errorf("weight %q is not an integer in [0..100]", p)
		}
		w[i] = v
		sum += v
	}
	if sum == 0 {
		return errors.New("weights sum to 0")
	}
	*out = w
	return nil
}

func text(env, def, doc string, field func(*Config) *string) Dial {
	return Dial{Env: env, Default: def, Doc: doc,
		apply: func(c *Config, raw string) error { *field(c) = raw; return nil }}
}

func integer(env, def, doc string, lo, hi int, field func(*Config) *int) Dial {
	return Dial{Env: env, Default: def, Doc: fmt.Sprintf("%s [%d..%d]", doc, lo, hi),
		apply: func(c *Config, raw string) error {
			v, err := strconv.Atoi(raw)
			if err != nil {
				return fmt.Errorf("not an integer: %q", raw)
			}
			if v < lo || v > hi {
				return fmt.Errorf("%d outside [%d..%d]", v, lo, hi)
			}
			*field(c) = v
			return nil
		}}
}

func duration(env, def, doc string, lo, hi time.Duration, field func(*Config) *time.Duration) Dial {
	return Dial{Env: env, Default: def, Doc: fmt.Sprintf("%s [%s..%s]", doc, lo, hi),
		apply: func(c *Config, raw string) error {
			v, err := time.ParseDuration(raw)
			if err != nil {
				return fmt.Errorf("not a duration: %q", raw)
			}
			if v < lo || v > hi {
				return fmt.Errorf("%s outside [%s..%s]", v, lo, hi)
			}
			*field(c) = v
			return nil
		}}
}
