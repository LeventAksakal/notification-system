// Command app is the service binary. One image, five entry points:
//
//	app serve        run the HTTP API until SIGINT/SIGTERM
//	                 (/openapi.json and /docs are served next to the API)
//	app worker       run a delivery worker until SIGINT/SIGTERM
//	app migrate      apply embedded migrations, then exit (compose runs it once)
//	app healthcheck  GET /readyz on localhost; the distroless image has no curl
//	app env          print every application setting as an env file, then exit
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	"github.com/LeventAksakal/notification-system/internal/api"
	"github.com/LeventAksakal/notification-system/internal/breaker"
	"github.com/LeventAksakal/notification-system/internal/config"
	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/limiter"
	"github.com/LeventAksakal/notification-system/internal/obs"
	"github.com/LeventAksakal/notification-system/internal/provider"
	"github.com/LeventAksakal/notification-system/internal/realtime"
	"github.com/LeventAksakal/notification-system/internal/store"
	delivery "github.com/LeventAksakal/notification-system/internal/worker"
)

func main() {
	cmd := "serve"
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
		// Reference only: no config, no connections, no telemetry.
		_, err := fmt.Fprint(os.Stdout, config.EnvExample())
		return err
	}
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	shutdownObs, err := obs.Setup(ctx, cfg.ServiceName, cfg.OTLPEndpoint, cfg.LogLevel)
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
	case "worker":
		return worker(ctx, cfg, db)
	default:
		return fmt.Errorf("unknown command %q (serve, worker, migrate, healthcheck, env)", cmd)
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

	breakers := make(map[core.Channel]*breaker.Breaker, len(core.Channels))
	for _, ch := range core.Channels {
		breakers[ch] = breaker.New(breaker.Config{
			Window:       cfg.BreakerWindow,
			MinRequests:  cfg.BreakerMinRequests,
			FailureRatio: cfg.BreakerFailureRatio,
			Cooldown:     cfg.BreakerCooldown,
		}, time.Now)
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
		SendAlign:       delivery.DefaultSendAlign,
	}, delivery.Deps{
		Queue:     store.NewQueue(db),
		Limiter:   limiter.New(vk, cfg.RateLimitPerSecond, cfg.RateLimitHeadroomPct, cfg.RateLimitHorizon, "ratelimit:"),
		Sender:    provider.New(cfg.ProviderURLs, cfg.ProviderTimeout, cfg.WorkerConcurrency),
		Publisher: realtime.NewPublisher(vk, realtime.OriginWorker),
		Breakers:  breakers,
		Tracer:    otel.Tracer("notifier/worker"),
	})
	slog.InfoContext(ctx, "worker started", "concurrency", cfg.WorkerConcurrency, "weights", cfg.PriorityWeights,
		"rate_per_second", cfg.RateLimitPerSecond, "headroom_percent", cfg.RateLimitHeadroomPct,
		"horizon", cfg.RateLimitHorizon.String())
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
	svc := intake.New(db, realtime.NewPublisher(vk, realtime.OriginAPI), cfg.IdempotencyRetention)
	srv := api.NewServer(svc, map[string]api.Check{
		"mysql": db.PingContext,
		// Ready only while events reach this instance: a subscriber accepted
		// now must not miss the next change.
		"valkey": func(ctx context.Context) error {
			if err := vk.Do(ctx, vk.B().Ping().Build()).Error(); err != nil {
				return err
			}
			if !hub.Live() {
				return errors.New("status event subscription not active")
			}
			return nil
		},
	}, api.Options{Metrics: store.NewMetricsReader(db), Hub: hub, WSSendBuffer: cfg.WSSendBuffer})
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
