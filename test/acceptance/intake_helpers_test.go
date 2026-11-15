//go:build integration

package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// The response shapes are the generated ones (test/apiclient, from
// api/openapi.yaml): apiclient.Notification, NotificationPage, BatchCreated,
// BatchStatus, StatusCounts, Template, Error and ErrorDetail. None is
// re-declared here.

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
func msg(channel, content string) apiclient.NotificationCreate {
	return apiclient.NotificationCreate{To: recipientFor(channel), Channel: apiclient.Channel(channel), Content: ptr(content)}
}

// reqOpt sets a request header on a typed create call.
type reqOpt func(*reqOpts)

type reqOpts struct {
	idempotencyKey, correlationID *string
	editors                       []apiclient.RequestEditorFn
}

// idemKey sends Idempotency-Key (a declared header parameter).
func idemKey(k string) reqOpt { return func(o *reqOpts) { o.idempotencyKey = &k } }

// corrID sends X-Correlation-ID (a declared header parameter).
func corrID(id string) reqOpt { return func(o *reqOpts) { o.correlationID = &id } }

// header sets a request header the contract does not declare on the
// operation: traceparent.
func header(name, value string) reqOpt {
	return func(o *reqOpts) { o.editors = append(o.editors, headerEditor(name, value)) }
}

// headerEditor sets one request header on a typed call.
func headerEditor(name, value string) apiclient.RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		req.Header.Set(name, value)
		return nil
	}
}

func collect(opts []reqOpt) reqOpts {
	var o reqOpts
	for _, f := range opts {
		f(&o)
	}
	return o
}

func (o reqOpts) createParams() *apiclient.CreateNotificationParams {
	return &apiclient.CreateNotificationParams{IdempotencyKey: o.idempotencyKey, XCorrelationID: o.correlationID}
}

func (o reqOpts) batchParams() *apiclient.CreateBatchParams {
	return &apiclient.CreateBatchParams{IdempotencyKey: o.idempotencyKey, XCorrelationID: o.correlationID}
}

// createResp posts one notification through the typed client; the caller
// checks the answer.
func (s *stack) createResp(t *testing.T, body apiclient.NotificationCreate, opts ...reqOpt) *apiclient.CreateNotificationResponse {
	t.Helper()
	o := collect(opts)
	r, err := s.API.CreateNotificationWithResponse(t.Context(), o.createParams(), body, o.editors...)
	if err != nil {
		t.Fatalf("POST /v1/notifications %s: %v", jsonOf(body), err)
	}
	return r
}

// create posts one notification, requires 201 and returns it.
func (s *stack) create(t *testing.T, body apiclient.NotificationCreate, opts ...reqOpt) apiclient.Notification {
	t.Helper()
	r := s.createResp(t, body, opts...)
	if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
		t.Fatalf("POST /v1/notifications %s: want 201, got %s", jsonOf(body), show(r))
	}
	if r.JSON201.Id == uuid.Nil {
		t.Fatalf("POST /v1/notifications: 201 without an id: %s", show(r))
	}
	return *r.JSON201
}

// createBatchResp posts a batch through the typed client; the caller checks
// the answer.
func (s *stack) createBatchResp(t *testing.T, items []apiclient.NotificationCreate, opts ...reqOpt) *apiclient.CreateBatchResponse {
	t.Helper()
	o := collect(opts)
	r, err := s.API.CreateBatchWithResponse(t.Context(), o.batchParams(), apiclient.BatchCreate{Notifications: items}, o.editors...)
	if err != nil {
		t.Fatalf("POST /v1/notifications/batch (%d items): %v", len(items), err)
	}
	return r
}

// createBatch posts a batch, requires 201 and returns it.
func (s *stack) createBatch(t *testing.T, items []apiclient.NotificationCreate, opts ...reqOpt) apiclient.BatchCreated {
	t.Helper()
	r := s.createBatchResp(t, items, opts...)
	if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
		t.Fatalf("POST /v1/notifications/batch (%d items): want 201, got %s", len(items), show(r))
	}
	return *r.JSON201
}

// get fetches one notification, requires 200.
func (s *stack) get(t *testing.T, id uuid.UUID) apiclient.Notification {
	t.Helper()
	r, err := s.API.GetNotificationWithResponse(t.Context(), id)
	if err != nil {
		t.Fatalf("GET /v1/notifications/%s: %v", id, err)
	}
	if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
		t.Fatalf("GET /v1/notifications/%s: want 200, got %s", id, show(r))
	}
	return *r.JSON200
}

// cancel cancels one notification, requires 200 and returns it.
func (s *stack) cancel(t *testing.T, id uuid.UUID) apiclient.Notification {
	t.Helper()
	r, err := s.API.CancelNotificationWithResponse(t.Context(), id)
	if err != nil {
		t.Fatalf("cancel %s: %v", id, err)
	}
	if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
		t.Fatalf("cancel %s: want 200, got %s", id, show(r))
	}
	return *r.JSON200
}

// getBatch reads one page of a batch, requires 200.
func (s *stack) getBatch(t *testing.T, id uuid.UUID, p apiclient.GetBatchParams) apiclient.BatchStatus {
	t.Helper()
	r, err := s.API.GetBatchWithResponse(t.Context(), id, &p)
	if err != nil {
		t.Fatalf("GET /v1/batches/%s: %v", id, err)
	}
	if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
		t.Fatalf("GET /v1/batches/%s: want 200, got %s", id, show(r))
	}
	return *r.JSON200
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
func (s *stack) storedStatus(t *testing.T, id uuid.UUID) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var st string
	if err := s.DB.QueryRowContext(ctx, "SELECT status FROM notifications WHERE id = ?", id.String()).Scan(&st); err != nil {
		t.Fatalf("status of %s: %v", id, err)
	}
	return st
}

// wantError requires an Error body with this status and code; each field in
// fields must be named by some details[].field.
func wantError(t *testing.T, r typedAnswer, status int, code string, fields ...string) apiclient.Error {
	t.Helper()
	if r.StatusCode() != status {
		t.Fatalf("want %d %s, got %s", status, code, show(r))
	}
	var e apiclient.Error
	if err := json.Unmarshal(r.GetBody(), &e); err != nil {
		t.Fatalf("want an Error body, got %s (%v)", show(r), err)
	}
	if e.Code != code {
		t.Errorf("error code: want %q, got %q (%s)", code, e.Code, show(r))
	}
	for _, f := range fields {
		if !names(e, f) {
			t.Errorf("details do not name field %q: %s", f, show(r))
		}
	}
	return e
}

// names reports whether some details[].field of e is field.
func names(e apiclient.Error, field string) bool {
	return slices.ContainsFunc(deref(e.Details), func(d apiclient.ErrorDetail) bool { return d.Field == field })
}

// requireIntake is the baseline of a test made mostly of refusals: unless
// intake accepts a plain valid notification, a refusal proves nothing.
func requireIntake(t *testing.T, s *stack) {
	t.Helper()
	s.create(t, msg("sms", "baseline"))
}

// newerFirst reports whether a comes strictly before b in "newest first"
// order: createdAt descending, then id descending.
func newerFirst(a, b apiclient.Notification) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.After(b.CreatedAt)
	}
	return strings.Compare(a.Id.String(), b.Id.String()) > 0
}

func ids(ns []apiclient.Notification) []uuid.UUID {
	out := make([]uuid.UUID, len(ns))
	for i, n := range ns {
		out[i] = n.Id
	}
	return out
}

// history is the attempt history of a notification read by id.
func history(n apiclient.Notification) []apiclient.DeliveryAttempt {
	return deref(n.DeliveryAttempts)
}

func attemptString(a apiclient.DeliveryAttempt) string {
	st := "none"
	if a.HttpStatus != nil {
		st = fmt.Sprint(*a.HttpStatus)
	}
	return fmt.Sprintf("#%d %s http=%s err=%q", a.AttemptNo, a.Outcome, st, deref(a.Error))
}

func attemptsString(as []apiclient.DeliveryAttempt) string {
	out := make([]string, len(as))
	for i, a := range as {
		out[i] = attemptString(a)
	}
	return "[" + strings.Join(out, ", ") + "]"
}

// postCreate posts one notification through the typed client and returns
// the answer unchecked, for the refusals.
func (s *stack) postCreate(t *testing.T, body apiclient.NotificationCreate, opts ...reqOpt) typedAnswer {
	t.Helper()
	return s.createResp(t, body, opts...)
}
