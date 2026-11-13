// Package config is the one place the service reads its environment. Every
// dial has a default, bounds and a doc line; config_test.go enforces that.
// Secrets (the DSN) have no default: they are injected at runtime, never
// committed.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"
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
}

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
}

// Load reads every dial through getenv (os.Getenv in main, a map in tests)
// and returns all problems at once.
func Load(getenv func(string) string) (Config, error) {
	var c Config
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
	return c, errors.Join(errs...)
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
