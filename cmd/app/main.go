// Command app is the service binary. One image, five entry points:
//
//	app serve        run the HTTP API until SIGINT/SIGTERM
//	                 (/openapi.json and /docs are served next to the API)
//	app worker       run a delivery worker until SIGINT/SIGTERM
//	app migrate      apply embedded migrations, then exit (compose runs it once)
//	app healthcheck  GET /readyz on localhost; the distroless image has no curl
//	app env          print every application setting as an env file, then exit
//	app env check    validate the environment against the settings, exit 0 or 1
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	"github.com/LeventAksakal/notification-system/config"
	delivery "github.com/LeventAksakal/notification-system/internal/delivery"
	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/delivery/limiter"
	"github.com/LeventAksakal/notification-system/internal/httpapi"
	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/provider"
	"github.com/LeventAksakal/notification-system/internal/realtime"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/internal/telemetry"
	"github.com/LeventAksakal/notification-system/internal/wake"
)

func main() {
	cmd := "" // no default role: a container started without one fails at once
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, cmd)
	stop()
	if err != nil {
		slog.Error("exit", "cmd", cmd, "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd string) error {
	switch cmd {
	case "healthcheck":
		return healthcheck(ctx, "http://127.0.0.1:8080/readyz")
	case "env":
		// No config, no connections, no telemetry.
		return env(os.Stdout, os.Args[2:], os.Environ()) //nolint:forbidigo // app env check validates the whole environment against config
	case "migrate", "serve", "worker":
	default:
		// Refused before config and connections: a typo is not a database error.
		return fmt.Errorf("%w, got %q", errUsage, cmd)
	}
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	shutdownObs, err := telemetry.Setup(ctx, cfg.ServiceName, cfg.OTLPEndpoint, cfg.LogLevel)
	if err != nil {
		return err
	}
	defer func() {
		// The signal has cancelled ctx by now; WithoutCancel keeps its values
		// but not its cancellation, so the flush gets its own 5 s.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := shutdownObs(ctx); err != nil {
			slog.Warn("telemetry flush", "err", err)
		}
	}()

	db, err := store.Open(ctx, cfg.DatabaseDSN, cfg.DBMaxOpenConns)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	switch cmd {
	case "migrate":
		return store.Migrate(ctx, db)
	case "serve":
		return serve(ctx, cfg, db)
	default: // "worker"; anything else was refused above
		return worker(ctx, cfg, db)
	}
}

// errUsage is the exit of a missing or unknown role.
var errUsage = errors.New("usage: app serve|worker|migrate|healthcheck|env [check]")

// errEnvCheck is the exit of an `app env check` that found problems; they
// are on stdout already.
var errEnvCheck = errors.New("env check: the environment does not match the settings")

// env prints the settings reference (no argument) or checks environ against
// it ("check"). It never prints a secret's value.
func env(out io.Writer, args, environ []string) error {
	switch {
	case len(args) == 0:
		_, err := fmt.Fprint(out, config.EnvExample())
		return err
	case len(args) == 1 && args[0] == "check":
		lines, ok := config.Check(environ)
		if _, err := fmt.Fprintln(out, strings.Join(lines, "\n")); err != nil {
			return err
		}
		if !ok {
			return errEnvCheck
		}
		return nil
	default:
		return fmt.Errorf("usage: app env [check], got %q", strings.Join(args, " "))
	}
}

func connectValkey(cfg config.Config) (valkey.Client, error) {
	vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{cfg.ValkeyAddr}, DisableCache: true})
	if err != nil {
		return nil, fmt.Errorf("valkey: %w", err)
	}
	return vk, nil
}

// worker is the delivery process: it runs until SIGINT/SIGTERM, then stops
// claiming, drains in-flight sends for APP_SHUTDOWN_TIMEOUT and releases the
// rest.
func worker(ctx context.Context, cfg config.Config, db *sql.DB) error {
	vk, err := connectValkey(cfg)
	if err != nil {
		return err
	}
	defer vk.Close()
	if err := store.Warm(ctx, db, cfg.DBMaxOpenConns); err != nil {
		return err
	}

	w := delivery.New(delivery.Config{
		Concurrency:     cfg.WorkerConcurrency,
		MaxAttempts:     cfg.MaxAttempts,
		RetryBase:       cfg.RetryBase,
		RetryCap:        cfg.RetryCap,
		Lease:           cfg.LeaseDuration,
		ReapInterval:    cfg.ReapInterval,
		ShutdownTimeout: cfg.ShutdownTimeout,
		Weights:         cfg.PriorityWeights,
		Horizon:         cfg.RateLimitHorizon,
		ProviderTimeout: cfg.ProviderTimeout,
		HealthCooldown:  cfg.BreakerCooldown,
	}, delivery.Deps{
		Queue:     store.NewQueue(db),
		Limiter:   limiter.New(vk, cfg.RateLimitPerSecond, cfg.RateLimitHeadroomPct, "ratelimit:"),
		Sender:    provider.New(cfg.ProviderURLs, cfg.ProviderTimeout, cfg.WorkerConcurrency),
		Publisher: realtime.NewPublisher(vk),
		Health:    health.NewValkey(vk, health.Config{Failures: cfg.BreakerFailures, Cooldown: cfg.BreakerCooldown}, "health:"),
		Tracer:    otel.Tracer("notifier/worker"),
		Waker:     wake.NewPublisher(vk),
		Wakes:     wake.NewListener(vk),
	})
	retryWorst, retryMean := delivery.RetrySpan(cfg.MaxAttempts, cfg.RetryBase, cfg.RetryCap)
	slog.InfoContext(ctx, "worker started", "concurrency", cfg.WorkerConcurrency, "weights", cfg.PriorityWeights,
		"max_attempts", cfg.MaxAttempts, "retry_span_mean", retryMean.String(), "retry_span_worst", retryWorst.String(),
		"rate_per_second", cfg.RateLimitPerSecond, "headroom_percent", cfg.RateLimitHeadroomPct,
		"horizon", cfg.RateLimitHorizon.String())
	if cfg.MaxAttempts < cfg.BreakerFailures {
		// Allowed (a deliberate fail-fast choice), but a lone message then
		// spends its whole budget before its outage can be declared (0026).
		slog.WarnContext(ctx, "retry budget below the outage threshold: a lone message is dead-lettered before an outage is detected",
			"max_attempts", cfg.MaxAttempts, "breaker_failures", cfg.BreakerFailures)
	}
	err = w.Run(ctx)
	slog.InfoContext(context.WithoutCancel(ctx), "worker stopped")
	return err
}

func serve(ctx context.Context, cfg config.Config, db *sql.DB) error {
	vk, err := connectValkey(cfg)
	if err != nil {
		return err
	}
	defer vk.Close()

	// One Valkey subscription per process feeds every WebSocket (decision 0013).
	hub := realtime.NewHub(vk)
	svc := intake.New(db, realtime.NewPublisher(vk), cfg.IdempotencyRetention).WithWaker(wake.NewPublisher(vk))
	// Readiness is MySQL only: intake, reads and cancels need nothing else.
	// While the hub's Valkey subscription is down, /v1/ws refuses new
	// subscribers with 503 and open streams are closed (decision 0013), so
	// live updates degrade on their own without taking intake out of rotation.
	srv := httpapi.NewServer(svc, map[string]httpapi.Check{
		"mysql": db.PingContext,
	}, httpapi.Options{Metrics: store.NewMetricsReader(db), Hub: hub, WSSendBuffer: cfg.WSSendBuffer,
		RequestTimeout: cfg.RequestTimeout})
	h, err := srv.Handler()
	if err != nil {
		return err
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	slog.InfoContext(ctx, "serving", "addr", ln.Addr().String())
	// The sweep and the hub live and stop with the server: a server error
	// cancels them. Open WebSockets are hijacked connections that
	// http.Server.Shutdown does not wait for; CloseStreams ends them.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return serveHTTP(gctx, ln, &http.Server{Handler: h, ReadHeaderTimeout: cfg.ReadHeaderTimeout}, cfg.ShutdownTimeout)
	})
	g.Go(func() error { return svc.SweepIdempotencyKeys(gctx, cfg.IdempotencySweep) })
	g.Go(func() error { return svc.SweepFinished(gctx, cfg.IdempotencySweep, cfg.NotificationRetention) })
	g.Go(func() error { return hub.Run(gctx) })
	g.Go(func() error {
		<-gctx.Done()
		srv.CloseStreams()
		return nil
	})
	return g.Wait()
}

// serveHTTP runs srv until ctx is cancelled, then drains in-flight requests
// for up to grace. It returns only after the server goroutine has exited.
func serveHTTP(ctx context.Context, ln net.Listener, srv *http.Server, grace time.Duration) error {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		// Drain on a fresh deadline: gctx is already cancelled.
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), grace)
		defer cancel()
		slog.InfoContext(ctx, "shutting down", "grace", grace.String())
		return srv.Shutdown(sctx)
	})
	return g.Wait()
}

func healthcheck(ctx context.Context, url string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, res.Status)
	}
	return nil
}
