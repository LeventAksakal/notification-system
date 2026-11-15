package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"uuid"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/validation"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/telemetry"
)

type CreateHeaders struct {
	IdempotencyKey idempotencyKey `header:"Idempotency-Key"`
	CorrelationID  correlationID  `header:"X-Correlation-ID"`
}

type createNotificationInput struct {
	CreateHeaders
	Body NotificationCreate
}

func (*createNotificationInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

type CreatedHeaders struct {
	CorrelationID string  `header:"X-Correlation-ID" doc:"The correlation id in force for this request"`
	Replayed      *string "header:\"Idempotent-Replayed\" enum:\"true\" nullable:\"false\" doc:\"Present and `true` when this response repeats an earlier one for the same Idempotency-Key\""
}

type createNotificationOutput struct {
	CreatedHeaders
	Body Notification
}

func (s *Server) CreateNotification(ctx context.Context, in *createNotificationInput) (*createNotificationOutput, error) {
	n, replayed, err := s.intake.Create(ctx, toRequest(in.Body), meta(ctx, in.IdempotencyKey))
	if err := refusal(err); err != nil {
		return nil, err
	}
	return &createNotificationOutput{CreatedHeaders: created(ctx, replayed), Body: toNotification(n)}, nil
}

type createBatchInput struct {
	CreateHeaders
	Body BatchCreate
}

func (*createBatchInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

type createBatchOutput struct {
	CreatedHeaders
	Body BatchCreated
}

func (s *Server) CreateBatch(ctx context.Context, in *createBatchInput) (*createBatchOutput, error) {
	rs := make([]intake.Request, len(in.Body.Notifications))
	for i, n := range in.Body.Notifications {
		rs[i] = toRequest(n)
	}
	b, replayed, err := s.intake.CreateBatch(ctx, rs, meta(ctx, in.IdempotencyKey))
	if err := refusal(err); err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, len(b.NotificationIDs))
	for i, id := range b.NotificationIDs {
		ids[i] = uuid.MustParse(id)
	}
	return &createBatchOutput{
		CreatedHeaders: created(ctx, replayed),
		Body:           BatchCreated{ID: uuid.MustParse(b.ID), Total: b.Total, NotificationIDs: ids, CreatedAt: b.CreatedAt},
	}, nil
}

type idInput struct {
	ID uuidParam `path:"id"`
}

type notificationOutput struct{ Body Notification }

func (s *Server) GetNotification(ctx context.Context, in *idInput) (*notificationOutput, error) {
	n, err := s.intake.Get(ctx, in.ID.String())
	if errors.Is(err, intake.ErrNotFound) {
		return nil, notFound("notification")
	}
	if err != nil {
		return nil, err
	}
	return &notificationOutput{toNotification(n)}, nil
}

type listNotificationsInput struct {
	Status      Status    "query:\"status\" doc:\"`scheduled` selects pending notifications whose due time is in the future and that were never tried; `pending` selects the other pending ones.\""
	Channel     Channel   `query:"channel"`
	BatchID     uuidParam `query:"batchId"`
	CreatedFrom time.Time "query:\"createdFrom\" doc:\"Inclusive lower bound on `createdAt`\""
	CreatedTo   time.Time "query:\"createdTo\" doc:\"Exclusive upper bound on `createdAt`\""
	Limit       pageLimit `query:"limit"`
	Cursor      cursor    `query:"cursor"`
}

// Resolve refuses a single-value parameter given twice (Huma reads the first).
func (*listNotificationsInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

type pageOutput struct{ Body NotificationPage }

func (s *Server) ListNotifications(ctx context.Context, in *listNotificationsInput) (*pageOutput, error) {
	f := intake.Filter{Status: string(in.Status), Channel: notification.Channel(in.Channel),
		Limit: int(in.Limit), Cursor: string(in.Cursor)}
	if in.BatchID != (uuidParam{}) {
		f.BatchID = in.BatchID.String()
	}
	if !in.CreatedFrom.IsZero() {
		f.CreatedFrom = &in.CreatedFrom
	}
	if !in.CreatedTo.IsZero() {
		f.CreatedTo = &in.CreatedTo
	}
	page, err := s.intake.List(ctx, f)
	if err := refusal(err); err != nil {
		return nil, err
	}
	out := &pageOutput{NotificationPage{Items: make([]Notification, len(page.Items)), NextCursor: page.NextCursor}}
	for i, n := range page.Items {
		out.Body.Items[i] = toNotification(n)
	}
	return out, nil
}

func (s *Server) CancelNotification(ctx context.Context, in *idInput) (*notificationOutput, error) {
	n, err := s.intake.Cancel(ctx, in.ID.String())
	switch {
	case errors.Is(err, intake.ErrNotFound):
		return nil, notFound("notification")
	case errors.Is(err, intake.ErrNotCancellable):
		return nil, newError(409, "not_cancellable", err.Error())
	case err != nil:
		return nil, err
	}
	return &notificationOutput{toNotification(n)}, nil
}

func (s *Server) RetryNotification(ctx context.Context, in *idInput) (*notificationOutput, error) {
	n, err := s.intake.Retry(ctx, in.ID.String())
	switch {
	case errors.Is(err, intake.ErrNotFound):
		return nil, notFound("notification")
	case errors.Is(err, intake.ErrNotRetryable):
		return nil, newError(409, "not_retryable", err.Error())
	case err != nil:
		return nil, err
	}
	return &notificationOutput{toNotification(n)}, nil
}

type getBatchInput struct {
	ID     uuidParam `path:"id"`
	Limit  pageLimit `query:"limit"`
	Cursor cursor    `query:"cursor"`
}

func (*getBatchInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

type batchOutput struct{ Body BatchStatus }

func (s *Server) GetBatch(ctx context.Context, in *getBatchInput) (*batchOutput, error) {
	b, err := s.intake.GetBatch(ctx, in.ID.String(), int(in.Limit), string(in.Cursor))
	if errors.Is(err, intake.ErrNotFound) {
		return nil, notFound("batch")
	}
	if err := refusal(err); err != nil {
		return nil, err
	}
	out := &batchOutput{BatchStatus{
		ID:         uuid.MustParse(b.ID),
		Total:      int(b.Total),
		CreatedAt:  b.CreatedAt,
		Counts:     toCounts(b.Counts),
		Items:      make([]Notification, len(b.Items)),
		NextCursor: b.NextCursor,
	}}
	for i, n := range b.Items {
		out.Body.Items[i] = toNotification(n)
	}
	return out, nil
}

// singleValues refuses what Huma lets through for a query or header
// parameter: a single-value one given more than once (Huma reads the first),
// and an empty value where the schema needs at least one character (Huma
// treats it as absent, so an empty Idempotency-Key would create without a
// key).
func singleValues(ctx huma.Context) []error {
	u := ctx.URL()
	q := u.Query()
	h := http.Header{}
	ctx.EachHeader(func(name, value string) { h.Add(name, value) })
	var errs []error
	for _, p := range ctx.Operation().Parameters {
		var vals []string
		switch {
		case p.In == "query" && (p.Schema == nil || p.Schema.Type != huma.TypeArray):
			vals = q[p.Name]
		case p.In == "header":
			vals = h.Values(p.Name)
		default:
			continue
		}
		switch {
		case len(vals) > 1:
			errs = append(errs, &huma.ErrorDetail{Location: p.In + "." + p.Name,
				Message: "expected one value, got " + strconv.Itoa(len(vals))})
		case len(vals) == 1 && vals[0] == "" && p.Schema != nil && p.Schema.MinLength != nil && *p.Schema.MinLength > 0:
			errs = append(errs, &huma.ErrorDetail{Location: p.In + "." + p.Name,
				Message: fmt.Sprintf(validation.MsgExpectedMinLength, *p.Schema.MinLength)})
		}
	}
	if nulls, ok := ctx.Context().Value(nullsKey{}).([]error); ok {
		errs = append(errs, nulls...)
	}
	return errs
}

// uuidParam is a UUID path or query parameter, read by parseUUID.
type uuidParam struct{ uuid.UUID }

func (u *uuidParam) UnmarshalText(b []byte) (err error) {
	u.UUID, err = parseUUID(string(b))
	return err
}

func (uuidParam) Schema(huma.Registry) *huma.Schema {
	return &huma.Schema{Type: huma.TypeString, Format: "uuid"}
}

// parseUUID reads a UUID as Huma's format check accepts it: the standard
// library trims a "urn:uuid:" prefix only in lower case.
func parseUUID(s string) (uuid.UUID, error) {
	if len(s) > 9 && strings.EqualFold(s[:9], "urn:uuid:") {
		s = s[9:]
	}
	return uuid.Parse(s)
}

// refusal maps the service's refusals to the contract's errors; any other
// error passes through to Huma, and so to toError (500, or 503 when the
// database cannot be reached).
func refusal(err error) error {
	var ve *intake.ValidationError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &ve):
		details := make([]ErrorDetail, len(ve.Details))
		for i, d := range ve.Details {
			details[i] = ErrorDetail{Index: d.Index, Field: d.Field, Reason: d.Reason}
		}
		e := invalid(details)
		e.Message = ve.Error()
		return e
	case errors.Is(err, intake.ErrIdempotencyKeyReused):
		return errKeyReused
	}
	return err
}

// meta collects what the service stores besides the body: the key, the
// correlation id the middleware put in the context, the active trace.
func meta(ctx context.Context, key idempotencyKey) intake.Meta {
	return intake.Meta{IdempotencyKey: string(key), CorrelationID: telemetry.CorrelationID(ctx),
		Traceparent: telemetry.Traceparent(ctx)}
}

func created(ctx context.Context, replayed bool) CreatedHeaders {
	h := CreatedHeaders{CorrelationID: telemetry.CorrelationID(ctx)}
	if replayed {
		h.Replayed = ptr("true")
	}
	return h
}

func toRequest(n NotificationCreate) intake.Request {
	r := intake.Request{To: n.To, Channel: notification.Channel(n.Channel),
		ScheduledAt: n.ScheduledAt, Priority: notification.Priority(n.Priority)}
	if n.Content != "" {
		r.Content = &n.Content
	}
	if n.Template != nil {
		r.Template = &intake.TemplateRef{Name: string(n.Template.Name), Variables: n.Template.Variables}
	}
	return r
}

func toCounts(c map[string]int) StatusCounts {
	return StatusCounts{
		Scheduled: c[string(StatusScheduled)], Pending: c[string(StatusPending)], Processing: c[string(StatusProcessing)],
		Sent: c[string(StatusSent)], Failed: c[string(StatusFailed)], Cancelled: c[string(StatusCancelled)],
	}
}

func toNotification(n intake.Notification) Notification {
	out := Notification{
		ID:            uuid.MustParse(n.ID),
		To:            n.Recipient,
		Channel:       Channel(n.Channel),
		Content:       n.Content,
		Priority:      Priority(n.Priority),
		Status:        Status(n.Status()),
		Attempts:      int(n.Attempts),
		DueAt:         n.DueAt,
		CorrelationID: n.CorrelationID,
		CreatedAt:     n.CreatedAt,
		UpdatedAt:     n.UpdatedAt,
	}
	if n.BatchID.Valid {
		out.BatchID = ptr(uuid.MustParse(n.BatchID.String))
	}
	if n.FailureKind.Valid {
		out.FailureKind = ptr(FailureKind(n.FailureKind.NotificationsFailureKind))
	}
	if n.LastError.Valid {
		out.LastError = &n.LastError.String
	}
	if n.ProviderMessageID.Valid {
		out.ProviderMessageID = &n.ProviderMessageID.String
	}
	out.ScheduledAt = timePtr(n.ScheduledAt.Time, n.ScheduledAt.Valid)
	out.FinishedAt = timePtr(n.FinishedAt.Time, n.FinishedAt.Valid)
	if n.History != nil {
		out.DeliveryAttempts = make([]DeliveryAttempt, len(n.History))
		for i, a := range n.History {
			d := DeliveryAttempt{
				AttemptNo:  int(a.AttemptNo),
				StartedAt:  a.StartedAt,
				DurationMs: int(a.DurationMs),
				Outcome:    string(a.Outcome),
			}
			if a.HttpStatus.Valid {
				d.HTTPStatus = ptr(int(a.HttpStatus.Int16))
			}
			if a.Error.Valid {
				d.Error = &a.Error.String
			}
			if a.ProviderMessageID.Valid {
				d.ProviderMessageID = &a.ProviderMessageID.String
			}
			out.DeliveryAttempts[i] = d
		}
	}
	return out
}

func timePtr(t time.Time, ok bool) *time.Time {
	if !ok {
		return nil
	}
	return &t
}
