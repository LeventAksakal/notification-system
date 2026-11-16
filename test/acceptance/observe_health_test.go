//go:build integration

package acceptance

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// R15: Health check endpoint - "Healthy when dependencies answer" (decision
// 0019: /healthz liveness; 0013 as changed 15 Nov 2026: /readyz checks MySQL
// only).
func TestR15HealthAndReadiness(t *testing.T) {
	s := startStack(t, stackOptions{})

	st, h, err := s.healthz()
	if err != nil || st != http.StatusOK || h.Status != "ok" {
		t.Errorf("/healthz: want 200 status ok, got %d %+v (%v)", st, h, err)
	}
	st, h, err = s.readyz()
	if err != nil || st != http.StatusOK || h.Status != "ok" {
		t.Errorf("/readyz: want 200 status ok, got %d %+v (%v)", st, h, err)
	}
	if h.Checks["mysql"] != "up" {
		t.Errorf("/readyz checks[\"mysql\"]: want \"up\", got %q (checks %v)", h.Checks["mysql"], h.Checks)
	}
}

// R15: Health check endpoint - "unready when the database ... is down"
// (decision 0013 as changed 15 Nov 2026: Valkey down no longer makes the
// instance unready; TestR15ReadyWithValkeyDownLiveUpdatesRefused covers it).
func TestR15ReadyzMySQLDown(t *testing.T) {
	s := startStack(t, stackOptions{})
	mysqlAddr := shared(t).mysqlAddr
	p := newTCPProxy(t, mysqlAddr)
	dsn := strings.Replace(s.DSN, "tcp("+mysqlAddr+")", "tcp("+p.Addr+")", 1)
	if dsn == s.DSN {
		t.Fatalf("cannot point DSN %q at the proxy", s.DSN)
	}
	peer := s.startServe(t, map[string]string{"APP_DATABASE_DSN": dsn})
	dependencyDown(t, peer, p, "mysql")
}

// dependencyDown closes the proxy in front of dep and expects /readyz to turn
// 503 unavailable with dep not up, while /healthz stays 200.
func dependencyDown(t *testing.T, peer *stack, p *tcpProxy, dep string) {
	t.Helper()
	st, h, err := peer.readyz()
	if err != nil || st != http.StatusOK || h.Checks[dep] != "up" {
		t.Fatalf("before the outage /readyz: want 200 with %s up, got %d %+v (%v)", dep, st, h, err)
	}

	p.Close()

	var last apiclient.Health
	var lastSt int
	if !poll(30*time.Second, func() bool {
		lastSt, last, err = peer.readyz()
		return err == nil && lastSt == http.StatusServiceUnavailable
	}) {
		t.Fatalf("not within 30s: /readyz with %s down: want 503, last %d %+v (%v)", dep, lastSt, last, err)
	}
	if last.Status != "unavailable" {
		t.Errorf("/readyz status: want unavailable, got %q", last.Status)
	}
	if last.Checks[dep] == "up" || last.Checks[dep] == "" {
		t.Errorf("/readyz checks[%q]: want a value other than up, got %q (checks %v)", dep, last.Checks[dep], last.Checks)
	}
	consistently(t, 2*time.Second, func() bool {
		st, h, err := peer.healthz()
		return err == nil && st == http.StatusOK && h.Status == "ok"
	}, "/healthz must stay 200 ok while %s is down", dep)
}
