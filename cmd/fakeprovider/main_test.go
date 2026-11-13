package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestSettingsComeFromTheEnvironmentWithDefaults(t *testing.T) {
	cases := []struct {
		name       string
		env        map[string]string
		want       config
		wantHealth string
		wantErr    bool
	}{
		{"defaults", nil, config{":8090", true}, "http://127.0.0.1:8090/healthz", false},
		{"custom port, dedupe off", map[string]string{"PROVIDER_ADDR": "0.0.0.0:9000", "PROVIDER_DEDUPE": "false"}, config{"0.0.0.0:9000", false}, "http://127.0.0.1:9000/healthz", false},
		{"empty values keep defaults", map[string]string{"PROVIDER_ADDR": "", "PROVIDER_DEDUPE": ""}, config{":8090", true}, "http://127.0.0.1:8090/healthz", false},
		{"dedupe not a bool", map[string]string{"PROVIDER_DEDUPE": "maybe"}, config{}, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := loadConfig(func(k string) (string, bool) { v, ok := tc.env[k]; return v, ok })
			if (err != nil) != tc.wantErr {
				t.Fatalf("err %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if got != tc.want || healthURL(got.addr) != tc.wantHealth {
				t.Errorf("got %+v (%s), want %+v (%s)", got, healthURL(got.addr), tc.want, tc.wantHealth)
			}
		})
	}
}

// SIGTERM ends a request hung by a fault rule at once, then nothing is left running.
func TestShutdownEndsHungRequestsAndLeaksNothing(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // run listens itself; free the port for it
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, nil, func(k string) (string, bool) {
			return map[string]string{"PROVIDER_ADDR": addr}[k], k == "PROVIDER_ADDR"
		})
	}()

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	waitHealthy(t, addr)
	put(t, client, "http://"+addr+"/_control/faults", `{"rules":[{"mode":"accept_then_hang","delayMs":60000}]}`)
	hung := make(chan struct{})
	go func() {
		defer close(hung)
		req, _ := http.NewRequestWithContext(context.WithoutCancel(ctx), http.MethodPost, "http://"+addr+"/", strings.NewReader(`{"to":"+90","channel":"sms","content":"x"}`))
		if res, err := client.Do(req); err == nil {
			_ = res.Body.Close()
		}
	}()
	waitFor(t, func() bool { // the hung request has been accepted (counted as a delivery)
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/_control/stats", nil)
		res, err := client.Do(req)
		if err != nil {
			return false
		}
		defer func() { _ = res.Body.Close() }()
		b, _ := io.ReadAll(res.Body)
		return strings.Contains(string(b), `"deliveries":1`)
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("shutdown waited for the hung request")
	}
	<-hung
}

func waitHealthy(t *testing.T, addr string) {
	t.Helper()
	waitFor(t, func() bool { return healthcheck(t.Context(), "http://"+addr+"/healthz") == nil })
}

// waitFor polls cond for up to 2 s; polling, not a fixed sleep, so it is as fast as the server.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for range 100 {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}

func put(t *testing.T, c *http.Client, url, body string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT %s: %s", url, res.Status)
	}
}
