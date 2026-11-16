//go:build integration

package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/config"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/realtime"
	"github.com/LeventAksakal/notification-system/internal/realtime/realtimetest"
	"github.com/LeventAksakal/notification-system/test/support"
)

// Decision 0013 as accepted on 15 Nov 2026: readiness is MySQL only. While
// Valkey is down the API stays ready and keeps creating, reading and
// cancelling; only /v1/ws refuses (503) and open streams are closed. When
// Valkey is back, after flapping, a new subscription gets its snapshot and
// then live events. A batch subscription skips what its items already show.
func TestServeStaysReadyWithValkeyDownAndOnlyLiveUpdatesDegrade(t *testing.T) {
	db, dsn := support.MySQL(t)
	vkAddr := support.Valkey(t)
	proxy := realtimetest.NewProxy(t, vkAddr)
	addr := freeAddr(t)
	env := map[string]string{"APP_DATABASE_DSN": dsn, "APP_VALKEY_ADDR": proxy.Addr, "APP_HTTP_ADDR": addr}
	cfg, err := config.Load(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := serve(ctx, cfg, db); err != nil && ctx.Err() == nil {
			t.Errorf("serve: %v", err)
		}
	}()
	defer func() { cancel(); wg.Wait() }()
	base := "http://" + addr
	c := &http.Client{Timeout: 5 * time.Second}

	until(t, 30*time.Second, "/readyz 200 at start", func() bool { return status(ctx, c, http.MethodGet, base+"/readyz", "") == http.StatusOK })
	id := create(ctx, t, c, base)
	wsURL := "ws://" + addr + "/v1/ws?notificationId=" + id
	var open *websocket.Conn
	until(t, 15*time.Second, "a subscription at start", func() bool {
		conn, res, err := websocket.Dial(ctx, wsURL, nil)
		if res != nil && res.Body != nil {
			_ = res.Body.Close()
		}
		open = conn
		return err == nil
	})
	if _, _, err := open.Read(ctx); err != nil { // the snapshot
		t.Fatalf("snapshot: %v", err)
	}

	t.Run("a batch subscription skips what its items already show", func(t *testing.T) {
		at := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		item := `{"to":"+905551234567","channel":"sms","content":"hi","scheduledAt":"` + at + `"}`
		var b struct {
			ID              string
			NotificationIDs []string
		}
		post(ctx, t, c, base+"/v1/notifications/batch", `{"notifications":[`+item+`,`+item+`]}`, http.StatusCreated, &b)
		sub, res, err := websocket.Dial(ctx, "ws://"+addr+"/v1/ws?batchId="+b.ID, nil)
		if res != nil && res.Body != nil {
			_ = res.Body.Close()
		}
		if err != nil {
			t.Fatalf("batch subscription: %v", err)
		}
		defer func() { _ = sub.CloseNow() }()
		// A late copy of the item's creation (seq 1) reaches Valkey after the
		// subscriber read the batch: it must be skipped.
		vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{vkAddr}, DisableCache: true})
		if err != nil {
			t.Fatal(err)
		}
		defer vk.Close()
		stale := notification.Event{NotificationID: b.NotificationIDs[0], BatchID: b.ID, Status: notification.StatusPending,
			At: time.Now().UTC(), CorrelationID: "late", Seq: 1}
		if err := realtime.NewPublisher(vk).Publish(ctx, stale); err != nil {
			t.Fatal(err)
		}
		post(ctx, t, c, base+"/v1/notifications/"+b.NotificationIDs[0]+"/cancel", "", http.StatusOK, nil)
		got := readStatus(t, ctx, sub)
		if got.NotificationID != b.NotificationIDs[0] || got.Status != "cancelled" {
			t.Fatalf("batch subscription, first message: got %+v, want the cancel of %s (the stale pending skipped)", got, b.NotificationIDs[0])
		}
	})

	proxy.Close()
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	_, _, err = open.Read(rctx)
	rcancel()
	if websocket.CloseStatus(err) != websocket.StatusServiceRestart {
		t.Fatalf("open stream with Valkey down: want closed with 1012 (feed lost), got %v", err)
	}

	t.Run("with Valkey down", func(t *testing.T) {
		start := time.Now()
		other := create(ctx, t, c, base)
		if took := time.Since(start); took > 3*time.Second {
			t.Errorf("create took %s with Valkey down (one bounded publish is 1 s)", took)
		}
		if st := status(ctx, c, http.MethodGet, base+"/readyz", ""); st != http.StatusOK {
			t.Errorf("/readyz: %d, want 200 (readiness is MySQL only)", st)
		}
		if st := status(ctx, c, http.MethodGet, base+"/v1/notifications/"+other, ""); st != http.StatusOK {
			t.Errorf("read: %d, want 200", st)
		}
		if st := status(ctx, c, http.MethodPost, base+"/v1/notifications/"+other+"/cancel", ""); st != http.StatusOK {
			t.Errorf("cancel: %d, want 200", st)
		}
		_, res, err := websocket.Dial(ctx, wsURL, nil)
		if err == nil || res == nil {
			t.Fatalf("subscribe with Valkey down: want 503, got err %v", err)
		}
		defer func() { _ = res.Body.Close() }()
		var body struct{ Code, Message string }
		_ = json.NewDecoder(res.Body).Decode(&body)
		if res.StatusCode != http.StatusServiceUnavailable || body.Code != "unavailable" || !strings.Contains(body.Message, "live updates are unavailable") {
			t.Fatalf("subscribe with Valkey down: got %d %+v, want 503 unavailable saying live updates are unavailable", res.StatusCode, body)
		}
	})

	// Valkey flaps: back, away again before or just after the hub
	// resubscribes, three times; then it stays.
	for range 3 {
		proxy.Reopen(t)
		time.Sleep(150 * time.Millisecond)
		proxy.Close()
		time.Sleep(150 * time.Millisecond)
	}
	proxy.Reopen(t)
	var back *websocket.Conn
	until(t, 15*time.Second, "a subscription after Valkey came back", func() bool {
		conn, res, err := websocket.Dial(ctx, wsURL, nil)
		if res != nil && res.Body != nil {
			_ = res.Body.Close()
		}
		back = conn
		return err == nil
	})
	defer func() { _ = back.CloseNow() }()
	if snap := readStatus(t, ctx, back); snap.Snapshot == nil || !*snap.Snapshot {
		t.Fatalf("first message after Valkey came back: want the snapshot, got %+v", snap)
	}
	post(ctx, t, c, base+"/v1/notifications/"+id+"/cancel", "", http.StatusOK, nil)
	if got := readStatus(t, ctx, back); got.Status != "cancelled" || got.NotificationID != id {
		t.Fatalf("live event after Valkey came back: got %+v, want %s cancelled", got, id)
	}
}

type statusMessage struct {
	NotificationID string `json:"notificationId"`
	Status         string `json:"status"`
	Snapshot       *bool  `json:"snapshot"`
}

func readStatus(t *testing.T, ctx context.Context, conn *websocket.Conn) statusMessage {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, b, err := conn.Read(rctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m statusMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return m
}

// post sends body (JSON, or nothing) and wants status; out, when set, gets the decoded answer.
func post(ctx context.Context, t *testing.T, c *http.Client, url, body string, want int, out any) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != want {
		t.Fatalf("POST %s: %d %s, want %d", url, res.StatusCode, b, want)
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("decode %s: %v", b, err)
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func until(t *testing.T, limit time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("not within %s: %s", limit, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func status(ctx context.Context, c *http.Client, method, url, body string) int {
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return 0
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.Do(req)
	if err != nil {
		return 0
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	return res.StatusCode
}

// create queues a notification scheduled an hour ahead (nobody claims it) and
// returns its id.
func create(ctx context.Context, t *testing.T, c *http.Client, base string) string {
	t.Helper()
	at := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/notifications",
		strings.NewReader(`{"to":"+905551234567","channel":"sms","content":"hi","scheduledAt":"`+at+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	var n struct{ ID string }
	if err := json.NewDecoder(res.Body).Decode(&n); err != nil || res.StatusCode != http.StatusCreated || n.ID == "" {
		t.Fatalf("create: %d %v %+v", res.StatusCode, err, n)
	}
	return n.ID
}
