package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// SIGTERM drains a slow in-flight request, then every goroutine is gone.
func TestShutdownDrainsInFlightRequestsAndLeaksNothing(t *testing.T) {
	defer goleak.VerifyNone(t)

	started := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: time.Second}
	done := make(chan error, 1)
	go func() { done <- serveHTTP(ctx, ln, srv, 2*time.Second) }()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+ln.Addr().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp := make(chan int, 1)
	go func() {
		r, err := client.Do(req)
		if err != nil {
			resp <- 0
			return
		}
		_ = r.Body.Close()
		resp <- r.StatusCode
	}()
	<-started
	cancel() // what signal.NotifyContext does on SIGTERM

	if code := <-resp; code != http.StatusNoContent {
		t.Fatalf("in-flight request got %d, want 204", code)
	}
	if err := <-done; err != nil {
		t.Fatalf("serveHTTP: %v", err)
	}
}

func TestEnvPrintsTheReferenceOrChecksTheEnvironment(t *testing.T) {
	for name, tc := range map[string]struct {
		args, environ []string
		wantErr       bool
		want          string
	}{
		"no argument prints the reference": {want: "# APP_HTTP_ADDR=:8080\n"},
		"check passes":                     {args: []string{"check"}, environ: []string{"APP_DATABASE_DSN=u:pw-x9@tcp(db)/app"}, want: "APP_DATABASE_DSN=(set)  # environment\n"},
		"check fails on a typo":            {args: []string{"check"}, environ: []string{"APP_DATABASE_DSN=u:pw-x9@tcp(db)/app", "APP_LOGLEVEL=debug"}, wantErr: true, want: "APP_LOGLEVEL: unknown setting"},
		"an unknown argument is refused":   {args: []string{"chek"}, wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			var out strings.Builder
			err := env(&out, tc.args, tc.environ)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err %v, want error %v", err, tc.wantErr)
			}
			if !strings.Contains(out.String(), tc.want) {
				t.Errorf("output lacks %q:\n%s", tc.want, out.String())
			}
			if strings.Contains(out.String(), "pw-x9") {
				t.Errorf("output shows the secret:\n%s", out.String())
			}
		})
	}
}
