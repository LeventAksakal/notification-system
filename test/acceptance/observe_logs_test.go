//go:build integration

package acceptance

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// R14: Structured logging with correlation IDs - "One id given at the API
// appears on every log line of that notification in the API and the worker,
// and reaches the provider" (decision 0019; digest: field `correlation_id`,
// `notification_id` when known, JSON on stdout).
func TestR14CorrelationIDOnLogsAndProvider(t *testing.T) {
	s := startStack(t, stackOptions{Workers: 1})
	var contents []string

	check := func(t *testing.T, kv []string) {
		content := "r14-private-content-" + randHex(12)
		contents = append(contents, content)
		r := s.postJSON(t, "/v1/notifications", msg("sms", content), kv...)
		if r.Status != 201 {
			t.Fatalf("create: want 201, got %s", r)
		}
		var n notification
		r.decode(t, &n)
		corr := r.Header.Get("X-Correlation-ID")
		if len(kv) == 2 && corr != kv[1] {
			t.Errorf("X-Correlation-ID echoed: want %q, got %q", kv[1], corr)
		}
		if corr == "" {
			t.Fatalf("response carries no X-Correlation-ID header")
		}
		if n.CorrelationID != corr {
			t.Errorf("notification correlationId %q differs from the response header %q", n.CorrelationID, corr)
		}

		reqs := s.providerRequests(t, n.ID, 60*time.Second)
		for _, pr := range reqs {
			if pr.CorrelationID != corr {
				t.Errorf("provider request X-Correlation-ID: want %q, got %q", corr, pr.CorrelationID)
			}
		}
		s.waitStatus(t, n.ID, "sent", 30*time.Second)

		for _, role := range []string{"serve", "worker"} {
			var about []map[string]any
			if !poll(10*time.Second, func() bool { about = linesAbout(s.Logs(role), n.ID); return len(about) > 0 }) {
				t.Errorf("%s: no log line with notification_id=%s", role, n.ID)
				continue
			}
			for _, l := range about {
				if got := fieldString(l, "correlation_id"); got != corr {
					t.Errorf("%s line about %s: correlation_id want %q, got %q: %v", role, n.ID, corr, got, l)
				}
			}
		}
	}

	t.Run("given id", func(t *testing.T) {
		check(t, []string{"X-Correlation-ID", "r14-corr-" + randHex(16)})
	})
	t.Run("generated id", func(t *testing.T) {
		check(t, nil)
	})

	t.Run("every line is JSON with time, level, msg and no content", func(t *testing.T) {
		for _, role := range []string{"serve", "worker"} {
			lines := s.LogLines(role)
			if len(lines) == 0 {
				t.Errorf("%s wrote no log line", role)
			}
			for _, l := range lines {
				if strings.TrimSpace(l) == "" {
					continue
				}
				for _, c := range contents {
					if strings.Contains(l, c) {
						t.Errorf("%s log line contains notification content %q: %s", role, c, l)
					}
				}
				var m map[string]any
				if err := json.Unmarshal([]byte(l), &m); err != nil || m == nil {
					t.Errorf("%s log line is not a JSON object: %q", role, l)
					continue
				}
				for _, k := range []string{"time", "level", "msg"} {
					if _, ok := m[k]; !ok {
						t.Errorf("%s log line lacks %q: %s", role, k, l)
					}
				}
			}
		}
	})
}
