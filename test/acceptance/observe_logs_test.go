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

	// check creates one notification, with X-Correlation-ID given ("" for none).
	check := func(t *testing.T, given string) {
		content := "r14-private-content-" + randHex(12)
		contents = append(contents, content)
		var opts []reqOpt
		if given != "" {
			opts = append(opts, corrID(given))
		}
		r := s.createResp(t, msg("sms", content), opts...)
		if r.StatusCode() != 201 || r.JSON201 == nil {
			t.Fatalf("create: want 201, got %s", show(r))
		}
		n := *r.JSON201
		corr := r.HTTPResponse.Header.Get("X-Correlation-ID")
		if given != "" && corr != given {
			t.Errorf("X-Correlation-ID echoed: want %q, got %q", given, corr)
		}
		if corr == "" {
			t.Fatalf("response carries no X-Correlation-ID header")
		}
		if n.CorrelationId != corr {
			t.Errorf("notification correlationId %q differs from the response header %q", n.CorrelationId, corr)
		}

		reqs := s.providerRequests(t, n.Id, 60*time.Second)
		for _, pr := range reqs {
			if pr.CorrelationID != corr {
				t.Errorf("provider request X-Correlation-ID: want %q, got %q", corr, pr.CorrelationID)
			}
		}
		s.waitStatus(t, n.Id, "sent", 30*time.Second)

		for _, role := range []string{"serve", "worker"} {
			var about []map[string]any
			if !poll(10*time.Second, func() bool { about = linesAbout(s.Logs(role), n.Id.String()); return len(about) > 0 }) {
				t.Errorf("%s: no log line with notification_id=%s", role, n.Id)
				continue
			}
			for _, l := range about {
				if got := fieldString(l, "correlation_id"); got != corr {
					t.Errorf("%s line about %s: correlation_id want %q, got %q: %v", role, n.Id, corr, got, l)
				}
			}
		}
	}

	t.Run("given id", func(t *testing.T) {
		check(t, "r14-corr-"+randHex(16))
	})
	t.Run("generated id", func(t *testing.T) {
		check(t, "")
	})

	// Owner item 0006 #4, option (a): a later request about a notification (a
	// cancel carrying its own X-Correlation-ID B) logs the notification's STORED
	// id A in correlation_id; B may appear only as request_correlation_id.
	t.Run("cancel with another correlation id: lines about the notification keep the stored id", func(t *testing.T) {
		stored := "r14-stored-" + randHex(16)
		other := "r14-cancel-" + randHex(16)
		content := "r14-private-content-" + randHex(12)
		contents = append(contents, content)
		body := msg("sms", content)
		// Scheduled hours ahead so the running worker cannot claim it before the cancel.
		body.ScheduledAt = ptr(time.Now().UTC().Add(3 * time.Hour).Truncate(time.Second))
		n := s.create(t, body, corrID(stored))
		before := len(linesAbout(s.Logs("serve"), n.Id.String()))

		// The cancel operation declares no X-Correlation-ID parameter; the
		// header is set on the typed request.
		r, err := s.API.CancelNotificationWithResponse(t.Context(), n.Id, headerEditor("X-Correlation-ID", other))
		if err != nil {
			t.Fatalf("cancel: %v", err)
		}
		if r.StatusCode() != 200 {
			t.Fatalf("cancel: want 200, got %s", show(r))
		}
		if got := r.HTTPResponse.Header.Get("X-Correlation-ID"); got != other {
			t.Errorf("cancel response X-Correlation-ID: want the request's %q, got %q", other, got)
		}
		// The cancel must log at least one line about the notification, or this
		// case would pass on silence.
		var about []map[string]any
		if !poll(10*time.Second, func() bool { about = linesAbout(s.Logs("serve"), n.Id.String()); return len(about) > before }) {
			t.Fatalf("serve: no log line with notification_id=%s after the cancel (had %d before)", n.Id, before)
		}
		for _, l := range about {
			if got := fieldString(l, "correlation_id"); got != stored {
				t.Errorf("serve line about %s: correlation_id want the stored %q, got %q: %v", n.Id, stored, got, l)
			}
		}
		// The request's own id still finds the request (decision 0019, As
		// built): the cancel line names it as request_correlation_id, and the
		// cancel's access line carries it as correlation_id.
		cancelLine := false
		for _, l := range about[before:] {
			if fieldString(l, "msg") == "notification cancelled" {
				cancelLine = true
				if got := fieldString(l, "request_correlation_id"); got != other {
					t.Errorf("cancel line about %s: request_correlation_id want the request's %q, got %q: %v", n.Id, other, got, l)
				}
			}
		}
		if !cancelLine {
			t.Errorf("serve: no \"notification cancelled\" line about %s among %v", n.Id, about[before:])
		}
		var access []map[string]any
		poll(5*time.Second, func() bool {
			access = nil
			for _, l := range s.Logs("serve") {
				p := fieldString(l, "path")
				if fieldString(l, "msg") == "request" && strings.HasSuffix(p, "/cancel") && strings.Contains(p, n.Id.String()) {
					access = append(access, l)
				}
			}
			return len(access) > 0
		})
		if len(access) != 1 {
			t.Fatalf("serve: want one access line for the cancel of %s, got %d", n.Id, len(access))
		}
		if got := fieldString(access[0], "correlation_id"); got != other {
			t.Errorf("cancel access line: correlation_id want the request's %q, got %q: %v", other, got, access[0])
		}
	})

	// A replayed create (same Idempotency-Key and body, another
	// X-Correlation-ID) logs the stored id as correlation_id and the
	// replaying request's id as request_correlation_id.
	t.Run("replayed create: the stored id, and the replay's id as request_correlation_id", func(t *testing.T) {
		stored := "r14-first-" + randHex(16)
		replay := "r14-replay-" + randHex(16)
		key := "r14-key-" + randHex(16)
		content := "r14-private-content-" + randHex(12)
		contents = append(contents, content)
		body := msg("sms", content)
		body.ScheduledAt = ptr(time.Now().UTC().Add(3 * time.Hour).Truncate(time.Second))
		n := s.create(t, body, idemKey(key), corrID(stored))
		r := s.createResp(t, body, idemKey(key), corrID(replay))
		if r.StatusCode() != 201 || r.JSON201 == nil || r.JSON201.Id != n.Id {
			t.Fatalf("replay: want 201 with the same notification %s, got %s", n.Id, show(r))
		}
		var replayed []map[string]any
		if !poll(10*time.Second, func() bool {
			replayed = nil
			for _, l := range linesAbout(s.Logs("serve"), n.Id.String()) {
				if fieldString(l, "msg") == "notification create replayed" {
					replayed = append(replayed, l)
				}
			}
			return len(replayed) > 0
		}) {
			t.Fatalf("serve: no \"notification create replayed\" line about %s", n.Id)
		}
		for _, l := range replayed {
			if got := fieldString(l, "correlation_id"); got != stored {
				t.Errorf("replayed line: correlation_id want the stored %q, got %q: %v", stored, got, l)
			}
			if got := fieldString(l, "request_correlation_id"); got != replay {
				t.Errorf("replayed line: request_correlation_id want the replay's %q, got %q: %v", replay, got, l)
			}
		}
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
