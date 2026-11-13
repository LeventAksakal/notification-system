//go:build integration

package acceptance

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// notification is the API's Notification schema.
type notification struct {
	ID               string            `json:"id"`
	BatchID          string            `json:"batchId"`
	To               string            `json:"to"`
	Channel          string            `json:"channel"`
	Content          string            `json:"content"`
	Priority         string            `json:"priority"`
	Status           string            `json:"status"`
	FailureKind      string            `json:"failureKind"`
	Attempts         int               `json:"attempts"`
	LastError        string            `json:"lastError"`
	ScheduledAt      *time.Time        `json:"scheduledAt"`
	DueAt            time.Time         `json:"dueAt"`
	CorrelationID    string            `json:"correlationId"`
	CreatedAt        time.Time         `json:"createdAt"`
	UpdatedAt        time.Time         `json:"updatedAt"`
	DeliveryAttempts []json.RawMessage `json:"deliveryAttempts"`
}

type notificationPage struct {
	Items      []notification `json:"items"`
	NextCursor string         `json:"nextCursor"`
}

type batchCreated struct {
	ID              string    `json:"id"`
	Total           int       `json:"total"`
	NotificationIDs []string  `json:"notificationIds"`
	CreatedAt       time.Time `json:"createdAt"`
}

type statusCounts struct {
	Scheduled  int `json:"scheduled"`
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Sent       int `json:"sent"`
	Failed     int `json:"failed"`
	Cancelled  int `json:"cancelled"`
}

type batchStatus struct {
	ID         string         `json:"id"`
	Total      int            `json:"total"`
	Counts     statusCounts   `json:"counts"`
	Items      []notification `json:"items"`
	NextCursor string         `json:"nextCursor"`
	CreatedAt  time.Time      `json:"createdAt"`
}

type template struct {
	Name      string    `json:"name"`
	Body      string    `json:"body"`
	Variables []string  `json:"variables"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type errorDetail struct {
	Index  *int   `json:"index"`
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

type apiError struct {
	Code    string        `json:"code"`
	Message string        `json:"message"`
	Details []errorDetail `json:"details"`
}

// Valid recipients per channel.
const (
	smsTo   = "+905551234567"
	emailTo = "ada@example.com"
	pushTo  = "device-token-1"
)

func recipientFor(channel string) string {
	switch channel {
	case "sms":
		return smsTo
	case "email":
		return emailTo
	default:
		return pushTo
	}
}

// msg is a valid create body for channel with the given content.
func msg(channel, content string) map[string]any {
	return map[string]any{"to": recipientFor(channel), "channel": channel, "content": content}
}

// create posts one notification, requires 201 and returns it.
func (s *stack) create(t *testing.T, body map[string]any, kv ...string) notification {
	t.Helper()
	r := s.postJSON(t, "/v1/notifications", body, kv...)
	if r.Status != http.StatusCreated {
		t.Fatalf("POST /v1/notifications %v: want 201, got %s", body, r)
	}
	var n notification
	r.decode(t, &n)
	if n.ID == "" {
		t.Fatalf("POST /v1/notifications: 201 without an id: %s", r)
	}
	return n
}

// createBatch posts a batch, requires 201 and returns it.
func (s *stack) createBatch(t *testing.T, items []map[string]any, kv ...string) batchCreated {
	t.Helper()
	r := s.postJSON(t, "/v1/notifications/batch", map[string]any{"notifications": items}, kv...)
	if r.Status != http.StatusCreated {
		t.Fatalf("POST /v1/notifications/batch (%d items): want 201, got %s", len(items), r)
	}
	var b batchCreated
	r.decode(t, &b)
	return b
}

// get fetches one notification, requires 200.
func (s *stack) get(t *testing.T, id string) notification {
	t.Helper()
	r := s.getJSON(t, "/v1/notifications/"+id)
	if r.Status != http.StatusOK {
		t.Fatalf("GET /v1/notifications/%s: want 200, got %s", id, r)
	}
	var n notification
	r.decode(t, &n)
	return n
}

// cancel cancels one notification, requires 200 and returns it.
func (s *stack) cancel(t *testing.T, id string) notification {
	t.Helper()
	r := s.postJSON(t, "/v1/notifications/"+id+"/cancel", nil)
	if r.Status != http.StatusOK {
		t.Fatalf("cancel %s: want 200, got %s", id, r)
	}
	var n notification
	r.decode(t, &n)
	return n
}

// count runs a COUNT(*) query on the stack's database.
func (s *stack) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	if err := s.DB.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// storedStatus reads a notification's stored status straight from the database.
func (s *stack) storedStatus(t *testing.T, id string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var st string
	if err := s.DB.QueryRowContext(ctx, "SELECT status FROM notifications WHERE id = ?", id).Scan(&st); err != nil {
		t.Fatalf("status of %s: %v", id, err)
	}
	return st
}

// wantError requires an Error body with this status and code; each field in
// fields must be named by some details[].field.
func wantError(t *testing.T, r *response, status int, code string, fields ...string) apiError {
	t.Helper()
	if r.Status != status {
		t.Fatalf("want %d %s, got %s", status, code, r)
	}
	var e apiError
	if err := json.Unmarshal(r.Body, &e); err != nil {
		t.Fatalf("want an Error body, got %s (%v)", r, err)
	}
	if e.Code != code {
		t.Errorf("error code: want %q, got %q (%s)", code, e.Code, r)
	}
	for _, f := range fields {
		if !e.names(f) {
			t.Errorf("details do not name field %q: %s", f, r)
		}
	}
	return e
}

func (e apiError) names(field string) bool {
	return slices.ContainsFunc(e.Details, func(d errorDetail) bool { return d.Field == field })
}

// requireIntake is the baseline of a test made mostly of refusals: unless
// intake accepts a plain valid notification, a refusal proves nothing.
func requireIntake(t *testing.T, s *stack) {
	t.Helper()
	s.create(t, msg("sms", "baseline"))
}

// newerFirst reports whether a comes strictly before b in "newest first"
// order: createdAt descending, then id descending.
func newerFirst(a, b notification) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.After(b.CreatedAt)
	}
	return strings.Compare(a.ID, b.ID) > 0
}

func ids(ns []notification) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.ID
	}
	return out
}
