//go:build integration

package acceptance

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The harness itself: the stack starts, answers its probes, and a worker
// starts with its output captured.
func TestHarnessStartsStack(t *testing.T) {
	s := startStack(t, stackOptions{})
	for _, p := range []string{"/readyz", "/healthz"} {
		if r := s.getJSON(t, p); r.Status != http.StatusOK {
			t.Errorf("GET %s: want 200, got %s", p, r)
		}
	}
	n := s.StartWorker()
	if n != 1 {
		t.Errorf("first StartWorker: want 1, got %d", n)
	}
	role := "worker-1"
	eventually(t, 30*time.Second, func() bool { return len(s.LogLines(role)) > 0 },
		"%s wrote no output", role)
	if len(s.Logs("serve")) == 0 {
		t.Errorf("serve wrote no JSON log line; raw: %q", s.LogLines("serve"))
	}
}

// D4: API documentation: Swagger/OpenAPI specification
// (the spec is served and matches the handlers).
func TestD4SpecAndDocsAreServed(t *testing.T) {
	s := startStack(t, stackOptions{})

	r := s.getJSON(t, "/openapi.json")
	if r.Status != http.StatusOK {
		t.Fatalf("GET /openapi.json: want 200, got %s", r)
	}
	spec := r.object(t)
	paths, ok := spec["paths"].(map[string]any)
	if !ok {
		t.Fatalf("/openapi.json has no paths object: %s", r)
	}
	for _, p := range []string{"/v1/notifications", "/v1/notifications/batch", "/v1/notifications/{id}", "/v1/batches/{id}", "/v1/templates/{name}"} {
		if _, ok := paths[p]; !ok {
			t.Errorf("/openapi.json lacks path %s", p)
		}
	}
	if info, _ := spec["info"].(map[string]any); info["version"] != "1.0.0" {
		t.Errorf("/openapi.json info.version: want 1.0.0, got %v", info["version"])
	}

	d := s.getJSON(t, "/docs")
	if d.Status != http.StatusOK {
		t.Fatalf("GET /docs: want 200, got %s", d)
	}
	if ct := d.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("GET /docs Content-Type: want text/html, got %q", ct)
	}
	if !strings.Contains(strings.ToLower(string(d.Body)), "<html") {
		t.Errorf("GET /docs is not an HTML page: %s", truncate(d.Body))
	}
}

// D5: Database migrations: versioned schema changes
// (migrations apply from empty).
func TestD5MigrateFromEmpty(t *testing.T) {
	db, dsn := freshDB(t)
	tables := func() map[string]bool {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		rows, err := db.QueryContext(ctx,
			"SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
		if err != nil {
			t.Fatalf("list tables: %v", err)
		}
		defer func() { _ = rows.Close() }()
		out := map[string]bool{}
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out[n] = true
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		return out
	}
	want := []string{"notifications", "batches", "delivery_attempts", "idempotency_keys", "templates"}
	before := tables()
	for _, n := range want {
		if before[n] {
			t.Fatalf("table %s exists before migrate; the database is not empty", n)
		}
	}

	env := childEnv(map[string]string{"APP_DATABASE_DSN": dsn})
	runApp(t, env, "migrate")
	after := tables()
	for _, n := range want {
		if !after[n] {
			t.Errorf("after app migrate on an empty database: table %s missing (have %v)", n, after)
		}
	}
	// A second run is a no-op, not an error.
	runApp(t, env, "migrate")
}
