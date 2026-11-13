// Command fakeprovider serves the provider double for the compose stack and
// manual runs. It is a test tool, not part of the service, so it reads its two
// settings itself instead of through internal/config:
//
//	PROVIDER_ADDR    listen address (default :8090)
//	PROVIDER_DEDUPE  true|false, Idempotency-Key dedupe at start (default true)
//
//	fakeprovider              serve until SIGINT/SIGTERM
//	fakeprovider healthcheck  GET /healthz on localhost; the image has no curl
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

type config struct {
	addr   string
	dedupe bool
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.LookupEnv)
	stop()
	if err != nil {
		slog.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, lookup func(string) (string, bool)) error {
	cfg, err := loadConfig(lookup)
	if err != nil {
		return err
	}
	if len(args) > 0 && args[0] == "healthcheck" {
		return healthcheck(ctx, healthURL(cfg.addr))
	}
	if len(args) > 0 {
		return fmt.Errorf("unknown command %q (healthcheck, or none to serve)", args[0])
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	slog.InfoContext(ctx, "provider double serving", "addr", ln.Addr().String(), "dedupe", cfg.dedupe)
	srv := &http.Server{
		Handler:           fakeprovider.New(fakeprovider.WithDedupe(cfg.dedupe)).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		// Request contexts derive from ctx, so SIGTERM also ends scripted delays and hangs.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	return serve(ctx, ln, srv)
}

func loadConfig(lookup func(string) (string, bool)) (config, error) {
	cfg := config{addr: ":8090", dedupe: true}
	if v, ok := lookup("PROVIDER_ADDR"); ok && v != "" {
		cfg.addr = v
	}
	if v, ok := lookup("PROVIDER_DEDUPE"); ok && v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return config{}, fmt.Errorf("PROVIDER_DEDUPE=%q: want true or false", v)
		}
		cfg.dedupe = b
	}
	return cfg, nil
}

// healthURL turns a listen address (":8090", "0.0.0.0:8090") into a local URL.
func healthURL(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		port = "8090"
	}
	return "http://" + net.JoinHostPort("127.0.0.1", port) + "/healthz"
}

// serve runs srv until ctx is cancelled, then gives in-flight requests 5 s to finish.
func serve(ctx context.Context, ln net.Listener, srv *http.Server) error {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
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
