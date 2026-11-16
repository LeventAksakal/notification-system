// Package config is the one place the service reads its environment. Every
// dial has a default, bounds and a doc line; config_test.go enforces that.
// Secrets (the DSN) have no default: they are injected at runtime, never
// committed, either as the variable itself or as <NAME>_FILE, the path of a
// file mounted by the secrets manager.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/LeventAksakal/notification-system/internal/delivery/leasebudget"
	"github.com/LeventAksakal/notification-system/internal/notification"
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
	RequestTimeout    time.Duration
	ShutdownTimeout   time.Duration

	// ProviderURLs holds the provider endpoint of each channel.
	ProviderURLs    map[notification.Channel]string
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
	BreakerFailures      int
	BreakerCooldown      time.Duration
	IdempotencyRetention time.Duration
	IdempotencySweep     time.Duration
	// NotificationRetention is how long a finished notification and its
	// attempts are kept after finished_at (decision 0025).
	NotificationRetention time.Duration
	WSSendBuffer          int
}

// ProviderURL returns the provider endpoint for a channel.
func (c Config) ProviderURL(ch notification.Channel) string { return c.ProviderURLs[ch] }

// Dial describes one environment variable.
type Dial struct {
	Env      string
	Doc      string
	Default  string // "" with Required means the value must be injected
	Required bool
	Secret   bool
	// PassThrough marks a variable a library reads itself (the OpenTelemetry
	// SDK's OTEL_*): it is declared, documented and validated here, so a bad
	// value refuses to start instead of falling back silently, and its Default
	// is what the library does when it is unset. Config does not hold it.
	// A misspelt name is not caught: env check knows only APP_ names.
	PassThrough bool
	apply       func(c *Config, raw string) error
}

// Dials lists every setting. Order is the order of the docs.
var Dials = []Dial{
	text("APP_HTTP_ADDR", ":8080", "HTTP listen address. Default: the conventional unprivileged HTTP port", func(c *Config) *string { return &c.HTTPAddr }),
	{Env: "APP_DATABASE_DSN", Doc: "MySQL DSN (go-sql-driver format). Default: none, a secret is injected at runtime, never committed", Required: true, Secret: true,
		apply: func(c *Config, raw string) error { c.DatabaseDSN = raw; return nil }},
	text("APP_VALKEY_ADDR", "localhost:6379", "Valkey host:port. Default: Valkey's standard port 6379", func(c *Config) *string { return &c.ValkeyAddr }),
	text("APP_OTLP_ENDPOINT", "", "OTLP/HTTP base URL, e.g. http://otel:4318; empty disables export. Default: empty, so a bare run needs no collector "+
		"(compose sets OTLP/HTTP's standard port 4318)",
		func(c *Config) *string { return &c.OTLPEndpoint }),
	text("APP_SERVICE_NAME", "notifier", "service.name on every span, metric and log. Default: the product's name (a free choice; OpenTelemetry requires one)", func(c *Config) *string { return &c.ServiceName }),
	{Env: "OTEL_TRACES_SAMPLER", Default: "parentbased_always_on", PassThrough: true,
		Doc: "trace sampler, read by the OpenTelemetry SDK: " + strings.Join(traceSamplers, ", ") +
			". Keep it parentbased_*: the worker continues the trace stored at the API, so the API's decision must hold there " +
			"(production: parentbased_traceidratio, decision 0019). Default: the OpenTelemetry SDK's own default",
		apply: func(_ *Config, raw string) error {
			if !slices.Contains(traceSamplers, strings.ToLower(strings.TrimSpace(raw))) {
				return fmt.Errorf("unknown sampler %q (want one of %s)", raw, strings.Join(traceSamplers, ", "))
			}
			return nil
		}},
	{Env: "OTEL_TRACES_SAMPLER_ARG", Default: "1", PassThrough: true,
		Doc: "share of new traces the *traceidratio samplers keep, read by the OpenTelemetry SDK; ignored by the others. Default: 1, the SDK's own default [0..1]",
		apply: func(_ *Config, raw string) error {
			v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
			if err != nil {
				return fmt.Errorf("not a number: %q", raw)
			}
			if !(v >= 0 && v <= 1) { // NaN too: the SDK would then sample all or nothing, by CPU
				return fmt.Errorf("%g outside [0..1]", v)
			}
			return nil
		}},
	{Env: "APP_LOG_LEVEL", Doc: "debug, info, warn or error. Default: slog's default level", Default: "info",
		apply: func(c *Config, raw string) error { return c.LogLevel.UnmarshalText([]byte(raw)) }},
	integer("APP_DB_MAX_OPEN_CONNS", "10", "MySQL pool size per process. Default: a worker needs 3 claims + 1 reap + result "+
		"writes (142.5/s x 5 ms on a healthy disk) = 5 connections, doubled; 4 processes x 10 stay far under MySQL's max_connections "+
		"151; 24 measured worse on a slow disk (decision 0018)", 1, 200, func(c *Config) *int { return &c.DBMaxOpenConns }),
	duration("APP_READ_HEADER_TIMEOUT", "5s", "time a client has to send request headers. Default: headers take one round trip (under 1 s on a slow "+
		"mobile link); 5 s bounds a slow-header client, far under nginx's 60 s client_header_timeout", time.Second, time.Minute,
		func(c *Config) *time.Duration { return &c.ReadHeaderTimeout }),
	duration("APP_REQUEST_TIMEOUT", "10s", "longest a request may take before it is answered 503. Default: the slowest legitimate "+
		"request, an 8 MiB batch uploaded at 10 Mbit/s (6.7 s) + its insert + the 1 s publish budget, rounded up; under proxies' 60 s", time.Second, 5*time.Minute,
		func(c *Config) *time.Duration { return &c.RequestTimeout }),
	duration("APP_SHUTDOWN_TIMEOUT", "10s", "grace period for in-flight requests and provider calls on SIGTERM; at least APP_REQUEST_TIMEOUT and "+
		"APP_PROVIDER_TIMEOUT. Default: the larger of those two; compose's 25 s stop_grace_period adds the 5 s telemetry flush, "+
		"5 s for final writes", time.Second, 2*time.Minute,
		func(c *Config) *time.Duration { return &c.ShutdownTimeout }),

	providerURL("APP_PROVIDER_URL_SMS", notification.ChannelSMS),
	providerURL("APP_PROVIDER_URL_EMAIL", notification.ChannelEmail),
	providerURL("APP_PROVIDER_URL_PUSH", notification.ChannelPush),
	duration("APP_PROVIDER_TIMEOUT", "5s", "time limit of one provider call. Default: 10 x the assumed provider p99 of 500 ms (decision 0018), so a "+
		"slow success is rarely cut", 10*time.Millisecond, time.Minute,
		func(c *Config) *time.Duration { return &c.ProviderTimeout }),
	integer("APP_MAX_ATTEMPTS", "10", "failed calls counted against a notification (made while its provider was believed healthy) "+
		"before it fails as exhausted. Default: the smallest budget whose waits reach APP_RETRY_CAP, ceil(log2(cap/base))+1 = 10 with "+
		"the default base and cap (a message-specific failure is retried for 6.75 min on average, 13.5 at most); also at least "+
		"APP_BREAKER_FAILURES, so a lone message survives the detection of its outage", 1, 50,
		func(c *Config) *int { return &c.MaxAttempts }),
	duration("APP_RETRY_BASE", "1s", "backoff base; after n counted failures the wait is random(0, min(cap, base*2^n)). "+
		"Default: the AWS SDK standard retry mode's base (full jitter, 1 s), so the first retry comes within 2 s", time.Millisecond, time.Minute,
		func(c *Config) *time.Duration { return &c.RetryBase }),
	duration("APP_RETRY_CAP", "5m", "backoff cap, the longest wait between two attempts; at least APP_RETRY_BASE. "+
		"Default target: once a message-specific failure clears, the message goes out within 5 min (time-sensitive "+
		"notifications such as one-time codes commonly expire in 5 to 10 min)", time.Millisecond, 24*time.Hour,
		func(c *Config) *time.Duration { return &c.RetryCap }),
	integer("APP_RATE_LIMIT_PER_SECOND", "100", "provider's send limit per channel, per second. Default: the brief's limit", 1, 100_000,
		func(c *Config) *int { return &c.RateLimitPerSecond }),
	integer("APP_RATE_LIMIT_HEADROOM_PERCENT", "5",
		"the limiter paces this many percent under the limit; send slots are exactly one interval apart, "+
			"so 0 is exact at the limiter but leaves no room for arrival jitter at the provider. Default: measured (owner item "+
			"0013): at 95 the busiest second at the provider is 98 to 99", 0, 50,
		func(c *Config) *int { return &c.RateLimitHeadroomPct }),
	duration("APP_RATE_LIMIT_HORIZON", "200ms", "least span of send slots one limiter call reserves past the send alignment; a busy channel calls the limiter every half of it "+
		"(200ms: 10 calls per second per channel per worker); it grows to the alignment while claims are slower than it. "+
		"Default: that budget of limiter calls (internal/delivery/send_alignment.go)",
		10*time.Millisecond, 2*time.Second,
		func(c *Config) *time.Duration { return &c.RateLimitHorizon }),
	{Env: "APP_PRIORITY_WEIGHTS", Default: "70,20,10",
		Doc: "high,normal,low shares of a channel's rate; each [0..100], sum above 0; unused share is borrowed. " +
			"A weight of 0 means that priority only sends on slots the others leave unused. " +
			"Default: a free choice (decision 0010); tune to the traffic mix",
		apply: func(c *Config, raw string) error { return parseWeights(raw, &c.PriorityWeights) }},
	duration("APP_LEASE_DURATION", "30s",
		"how long a claimed notification belongs to its worker; a provider call is cut "+leasebudget.Margin.String()+" before it "+
			"ends; at least APP_PROVIDER_TIMEOUT + the longest slot wait (a quarter of the lease) + one slot interval + "+
			leasebudget.Margin.String()+". Default: an eighth of it is the send alignment's ceiling, 3.75 s, above the claims of a "+
			"loaded database (about 3 s at p95, owner item 0011)", time.Second, time.Hour,
		func(c *Config) *time.Duration { return &c.LeaseDuration }),
	duration("APP_REAP_INTERVAL", "5s", "how often expired leases are returned to pending. Default: worst-case recovery after a crash is "+
		"lease + reap = 35 s; shorter adds little next to the 30 s lease, for one indexed UPDATE per worker per 5 s", 10*time.Millisecond, 10*time.Minute,
		func(c *Config) *time.Duration { return &c.ReapInterval }),
	integer("APP_WORKER_CONCURRENCY", "72", "provider calls in flight per worker process. Default: Little's law, 2 of 3 workers "+
		"carry the full 285/s at the assumed 500 ms provider p99: 285 x 0.5 / 2 = 72", 1, 1024,
		func(c *Config) *int { return &c.WorkerConcurrency }),
	integer("APP_BREAKER_FAILURES", "5", "consecutive provider-fault results on a channel, across all workers, that declare its "+
		"provider down (shared in Valkey, decision 0026); the declaring failure and every later one until recovery do not count "+
		"against a message's budget. Default: Envoy outlier detection's consecutive_5xx (5): an outage is declared within 5 failed "+
		"calls fleet-wide and costs at most 4 budget units; at 95/s a provider failing 10% of calls at random trips about once per "+
		"20 min", 1, 10_000,
		func(c *Config) *int { return &c.BreakerFailures }),
	duration("APP_BREAKER_COOLDOWN", "5s", "least time between an outage being declared (or a probe failing) and the next probe; "+
		"one probe per cool-down fleet-wide. Default target: sending resumes within 5 s plus one probe round trip of recovery "+
		"(Hystrix's default sleep window), for one call per 5 s while down (0.2% of the paced rate)", 10*time.Millisecond, 10*time.Minute,
		func(c *Config) *time.Duration { return &c.BreakerCooldown }),
	duration("APP_IDEMPOTENCY_RETENTION", "24h", "how long an Idempotency-Key is remembered. Default: Stripe's 24 h key window", time.Second, 30*24*time.Hour,
		func(c *Config) *time.Duration { return &c.IdempotencyRetention }),
	duration("APP_IDEMPOTENCY_SWEEP_INTERVAL", "1m", "how often app serve deletes expired Idempotency-Keys and notifications past APP_NOTIFICATION_RETENTION. "+
		"Default: expired keys linger at most 1 min, 0.07% of a 24 h table", 10*time.Millisecond, time.Hour,
		func(c *Config) *time.Duration { return &c.IdempotencySweep }),
	// At least the metrics window (an hour), so /metrics never counts a
	// deleted row; at least APP_IDEMPOTENCY_RETENTION, checked in load.
	duration("APP_NOTIFICATION_RETENTION", "168h", "how long sent, failed and cancelled notifications and their attempts are kept after they finish; a failed one can be replayed only while kept. "+
		"Default: a failure on a Friday is still replayable on Monday (decision 0025)", time.Hour, 365*24*time.Hour,
		func(c *Config) *time.Duration { return &c.NotificationRetention }),
	integer("APP_WS_SEND_BUFFER", "1024", "events buffered per WebSocket subscriber before it is dropped as slow. Default: one "+
		"batch create publishes up to 1000 events at once and an unfiltered stream gets 855 events/s at the full rate; 1024 holds "+
		"one batch burst (about 140 KB per socket, decision 0018)", 1, 65_536,
		func(c *Config) *int { return &c.WSSendBuffer }),
}

// traceSamplers are the OTEL_TRACES_SAMPLER values the OpenTelemetry Go SDK
// (sdk/trace, sampler_env.go) accepts; it lower-cases and trims the value,
// and falls back to parentbased_always_on on anything else.
var traceSamplers = []string{
	"always_on", "always_off", "traceidratio",
	"parentbased_always_on", "parentbased_always_off", "parentbased_traceidratio",
}

// samplerArgIgnored refuses a ratio given to a sampler that ignores it: with
// only OTEL_TRACES_SAMPLER_ARG=0.02 set, the default sampler keeps 100%.
func samplerArgIgnored(settings []setting) error {
	var sampler, arg setting
	for i, d := range Dials {
		switch d.Env {
		case "OTEL_TRACES_SAMPLER":
			sampler = settings[i]
		case "OTEL_TRACES_SAMPLER_ARG":
			arg = settings[i]
		}
	}
	if arg.src == SourceEnv && !strings.HasSuffix(strings.ToLower(strings.TrimSpace(sampler.raw)), "traceidratio") {
		return fmt.Errorf("OTEL_TRACES_SAMPLER_ARG %s is ignored by OTEL_TRACES_SAMPLER %s; use parentbased_traceidratio", arg.raw, sampler.raw)
	}
	return nil
}

// FileSuffix names the variable that holds the path of a secret's file: a
// secret setting NAME may instead be given as NAME_FILE=/path, the way a
// secrets manager mounts it (Kubernetes and Docker secrets, Vault agent).
const FileSuffix = "_FILE"

// Source says where a setting's value came from.
type Source string

// The sources of a value.
const (
	SourceDefault Source = "default"
	SourceEnv     Source = "environment"
	SourceFile    Source = "file"
)

// Load reads every dial through getenv (os.Getenv in main, a map in tests)
// and returns all problems at once. A secret dial may be given as
// <NAME>_FILE, a path whose content (one trailing newline trimmed) is the
// value; setting both forms is an error.
func Load(getenv func(string) string) (Config, error) {
	c, _, errs := load(getenv, os.ReadFile)
	return c, errors.Join(errs...)
}

// setting is one dial's raw value and where it came from.
type setting struct {
	raw string
	src Source
}

// load is Load with the file reader injected; it also returns each dial's
// raw value and source, in Dials order.
func load(getenv func(string) string, readFile func(string) ([]byte, error)) (Config, []setting, []error) {
	c := Config{ProviderURLs: make(map[notification.Channel]string, len(notification.Channels))}
	settings := make([]setting, len(Dials))
	var errs []error
	for i, d := range Dials {
		raw, src, err := resolve(d, getenv, readFile)
		settings[i] = setting{raw: raw, src: src}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if raw == "" && d.Required {
			errs = append(errs, fmt.Errorf("%s is required: %s", d.Env, d.Doc))
			continue
		}
		if err := d.apply(&c, raw); err != nil {
			if d.Secret {
				// A parse error may quote the value; a secret's is never shown.
				err = errors.New("invalid value (not shown: secret)")
			}
			errs = append(errs, fmt.Errorf("%s: %w", d.Env, err))
		}
	}
	if err := samplerArgIgnored(settings); err != nil {
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		errs = crossChecks(c)
	}
	return c, settings, errs
}

// resolve returns a dial's raw value and where it came from. Errors name the
// variable and the path, never a file's content.
func resolve(d Dial, getenv func(string) string, readFile func(string) ([]byte, error)) (string, Source, error) {
	raw := getenv(d.Env)
	if d.Secret {
		if path := getenv(d.Env + FileSuffix); path != "" {
			if raw != "" {
				return "", SourceFile, fmt.Errorf("%s and %s%s are both set; set one", d.Env, d.Env, FileSuffix)
			}
			b, err := readFile(path)
			if err != nil {
				var pe *fs.PathError
				if errors.As(err, &pe) {
					err = pe.Err
				}
				return "", SourceFile, fmt.Errorf("%s%s: cannot read %s: %w", d.Env, FileSuffix, path, err)
			}
			s := strings.TrimSuffix(string(b), "\n")
			s = strings.TrimSuffix(s, "\r") // a CRLF ending is one newline too
			if s == "" {
				return "", SourceFile, fmt.Errorf("%s%s: %s is empty", d.Env, FileSuffix, path)
			}
			return s, SourceFile, nil
		}
	}
	if raw != "" {
		return raw, SourceEnv, nil
	}
	return d.Default, SourceDefault, nil
}

func providerURL(env string, ch notification.Channel) Dial {
	return Dial{Env: env, Default: "http://localhost:8090/send", Doc: "provider endpoint for " + string(ch) + " (http or https URL). Default: the provider double's local address",
		apply: func(c *Config, raw string) error {
			u, err := url.Parse(raw)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return fmt.Errorf("not an http(s) URL: %q", raw)
			}
			c.ProviderURLs[ch] = raw
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
