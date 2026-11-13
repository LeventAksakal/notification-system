package api

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/obs"
)

func (s *Server) CreateNotification(ctx context.Context, req CreateNotificationRequestObject) (CreateNotificationResponseObject, error) {
	if req.Body == nil {
		return CreateNotification400JSONResponse{InvalidRequestJSONResponse(missingBody)}, nil
	}
	n, replayed, err := s.intake.Create(ctx, toRequest(*req.Body), meta(ctx, req.Params.IdempotencyKey))
	if body, ok := invalidBody(err); ok {
		return CreateNotification400JSONResponse{InvalidRequestJSONResponse(body)}, nil
	}
	if errors.Is(err, intake.ErrIdempotencyKeyReused) {
		return CreateNotification422JSONResponse{IdempotencyKeyReusedJSONResponse(keyReused)}, nil
	}
	if err != nil {
		return nil, err
	}
	cid := obs.CorrelationID(ctx)
	return CreateNotification201JSONResponse{
		Body:    toNotification(n),
		Headers: CreateNotification201ResponseHeaders{XCorrelationID: &cid, IdempotentReplayed: replayedHeader(replayed)},
	}, nil
}

func (s *Server) CreateBatch(ctx context.Context, req CreateBatchRequestObject) (CreateBatchResponseObject, error) {
	if req.Body == nil {
		return CreateBatch400JSONResponse{InvalidRequestJSONResponse(missingBody)}, nil
	}
	rs := make([]intake.Request, len(req.Body.Notifications))
	for i, n := range req.Body.Notifications {
		rs[i] = toRequest(n)
	}
	b, replayed, err := s.intake.CreateBatch(ctx, rs, meta(ctx, req.Params.IdempotencyKey))
	if body, ok := invalidBody(err); ok {
		return CreateBatch400JSONResponse{InvalidRequestJSONResponse(body)}, nil
	}
	if errors.Is(err, intake.ErrIdempotencyKeyReused) {
		return CreateBatch422JSONResponse{IdempotencyKeyReusedJSONResponse(keyReused)}, nil
	}
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, len(b.NotificationIDs))
	for i, id := range b.NotificationIDs {
		ids[i] = uuid.MustParse(id)
	}
	cid := obs.CorrelationID(ctx)
	return CreateBatch201JSONResponse{
		Body:    BatchCreated{Id: uuid.MustParse(b.ID), Total: b.Total, NotificationIds: ids, CreatedAt: b.CreatedAt},
		Headers: CreateBatch201ResponseHeaders{XCorrelationID: &cid, IdempotentReplayed: replayedHeader(replayed)},
	}, nil
}

func (s *Server) GetNotification(ctx context.Context, req GetNotificationRequestObject) (GetNotificationResponseObject, error) {
	n, err := s.intake.Get(ctx, req.Id.String())
	if errors.Is(err, intake.ErrNotFound) {
		return GetNotification404JSONResponse{NotFoundJSONResponse(notFound("notification"))}, nil
	}
	if err != nil {
		return nil, err
	}
	return GetNotification200JSONResponse(toNotification(n)), nil
}

func (s *Server) ListNotifications(ctx context.Context, req ListNotificationsRequestObject) (ListNotificationsResponseObject, error) {
	p := req.Params
	f := intake.Filter{CreatedFrom: p.CreatedFrom, CreatedTo: p.CreatedTo}
	if p.Status != nil {
		f.Status = string(*p.Status)
	}
	if p.Channel != nil {
		f.Channel = core.Channel(*p.Channel)
	}
	if p.BatchId != nil {
		f.BatchID = p.BatchId.String()
	}
	if p.Limit != nil {
		f.Limit = *p.Limit
	}
	if p.Cursor != nil {
		f.Cursor = *p.Cursor
	}
	page, err := s.intake.List(ctx, f)
	if body, ok := invalidBody(err); ok {
		return ListNotifications400JSONResponse{InvalidRequestJSONResponse(body)}, nil
	}
	if err != nil {
		return nil, err
	}
	out := ListNotifications200JSONResponse{Items: make([]Notification, len(page.Items))}
	for i, n := range page.Items {
		out.Items[i] = toNotification(n)
	}
	if page.NextCursor != "" {
		out.NextCursor = &page.NextCursor
	}
	return out, nil
}

func (s *Server) CancelNotification(ctx context.Context, req CancelNotificationRequestObject) (CancelNotificationResponseObject, error) {
	n, err := s.intake.Cancel(ctx, req.Id.String())
	switch {
	case errors.Is(err, intake.ErrNotFound):
		return CancelNotification404JSONResponse{NotFoundJSONResponse(notFound("notification"))}, nil
	case errors.Is(err, intake.ErrNotCancellable):
		return CancelNotification409JSONResponse{ConflictJSONResponse(Error{Code: "not_cancellable", Message: err.Error()})}, nil
	case err != nil:
		return nil, err
	}
	return CancelNotification200JSONResponse(toNotification(n)), nil
}

func (s *Server) RetryNotification(ctx context.Context, req RetryNotificationRequestObject) (RetryNotificationResponseObject, error) {
	n, err := s.intake.Retry(ctx, req.Id.String())
	switch {
	case errors.Is(err, intake.ErrNotFound):
		return RetryNotification404JSONResponse{NotFoundJSONResponse(notFound("notification"))}, nil
	case errors.Is(err, intake.ErrNotRetryable):
		return RetryNotification409JSONResponse{ConflictJSONResponse(Error{Code: "not_retryable", Message: err.Error()})}, nil
	case err != nil:
		return nil, err
	}
	return RetryNotification200JSONResponse(toNotification(n)), nil
}

func (s *Server) GetBatch(ctx context.Context, req GetBatchRequestObject) (GetBatchResponseObject, error) {
	var limit int
	var cursor string
	if req.Params.Limit != nil {
		limit = *req.Params.Limit
	}
	if req.Params.Cursor != nil {
		cursor = *req.Params.Cursor
	}
	b, err := s.intake.GetBatch(ctx, req.Id.String(), limit, cursor)
	if body, ok := invalidBody(err); ok {
		return GetBatch400JSONResponse{InvalidRequestJSONResponse(body)}, nil
	}
	if errors.Is(err, intake.ErrNotFound) {
		return GetBatch404JSONResponse{NotFoundJSONResponse(notFound("batch"))}, nil
	}
	if err != nil {
		return nil, err
	}
	out := GetBatch200JSONResponse{
		Id:        uuid.MustParse(b.ID),
		Total:     int(b.Total),
		CreatedAt: b.CreatedAt,
		Counts: StatusCounts{
			Scheduled:  b.Counts[string(Scheduled)],
			Pending:    b.Counts[string(Pending)],
			Processing: b.Counts[string(Processing)],
			Sent:       b.Counts[string(Sent)],
			Failed:     b.Counts[string(Failed)],
			Cancelled:  b.Counts[string(Cancelled)],
		},
		Items: make([]Notification, len(b.Items)),
	}
	for i, n := range b.Items {
		out.Items[i] = toNotification(n)
	}
	if b.NextCursor != "" {
		out.NextCursor = &b.NextCursor
	}
	return out, nil
}

// meta collects what the service stores besides the body: the key, the
// correlation id the middleware put in the context, the active trace.
func meta(ctx context.Context, key *string) intake.Meta {
	m := intake.Meta{CorrelationID: obs.CorrelationID(ctx), Traceparent: obs.Traceparent(ctx)}
	if key != nil {
		m.IdempotencyKey = *key
	}
	return m
}

func toRequest(n NotificationCreate) intake.Request {
	r := intake.Request{To: n.To, Channel: core.Channel(n.Channel), Content: n.Content, ScheduledAt: n.ScheduledAt}
	if n.Priority != nil {
		r.Priority = core.Priority(*n.Priority)
	}
	if n.Template != nil {
		r.Template = &intake.TemplateRef{Name: n.Template.Name}
		if n.Template.Variables != nil {
			r.Template.Variables = *n.Template.Variables
		}
	}
	return r
}

func toNotification(n intake.Notification) Notification {
	out := Notification{
		Id:            uuid.MustParse(n.ID),
		To:            n.Recipient,
		Channel:       Channel(n.Channel),
		Content:       n.Content,
		Priority:      Priority(n.Priority),
		Status:        Status(n.Status()),
		Attempts:      int(n.Attempts),
		DueAt:         n.DueAt,
		CorrelationId: n.CorrelationID,
		CreatedAt:     n.CreatedAt,
		UpdatedAt:     n.UpdatedAt,
	}
	if n.BatchID.Valid {
		id := uuid.MustParse(n.BatchID.String)
		out.BatchId = &id
	}
	if n.FailureKind.Valid {
		k := FailureKind(n.FailureKind.NotificationsFailureKind)
		out.FailureKind = &k
	}
	if n.LastError.Valid {
		out.LastError = &n.LastError.String
	}
	if n.ProviderMessageID.Valid {
		out.ProviderMessageId = &n.ProviderMessageID.String
	}
	out.ScheduledAt = timePtr(n.ScheduledAt.Time, n.ScheduledAt.Valid)
	out.FinishedAt = timePtr(n.FinishedAt.Time, n.FinishedAt.Valid)
	if n.History != nil {
		attempts := make([]DeliveryAttempt, len(n.History))
		for i, a := range n.History {
			d := DeliveryAttempt{
				AttemptNo:  int(a.AttemptNo),
				StartedAt:  a.StartedAt,
				DurationMs: int(a.DurationMs),
				Outcome:    DeliveryAttemptOutcome(a.Outcome),
			}
			if a.HttpStatus.Valid {
				st := int(a.HttpStatus.Int16)
				d.HttpStatus = &st
			}
			if a.Error.Valid {
				d.Error = &a.Error.String
			}
			if a.ProviderMessageID.Valid {
				d.ProviderMessageId = &a.ProviderMessageID.String
			}
			attempts[i] = d
		}
		out.DeliveryAttempts = &attempts
	}
	return out
}

func timePtr(t time.Time, ok bool) *time.Time {
	if !ok {
		return nil
	}
	return &t
}

func replayedHeader(replayed bool) *string {
	if !replayed {
		return nil
	}
	t := "true"
	return &t
}

var (
	missingBody = Error{Code: "invalid_request", Message: "body: a JSON body is required",
		Details: &[]ErrorDetail{{Field: "body", Reason: "required"}}}
	keyReused = Error{Code: "idempotency_key_reused",
		Message: "this Idempotency-Key was used before with a different payload"}
)

func notFound(what string) Error {
	return Error{Code: "not_found", Message: "no such " + what}
}

// invalidBody turns the service's validation error into the contract's
// 400 body (the same shape the spec validator writes).
func invalidBody(err error) (Error, bool) {
	var ve *intake.ValidationError
	if !errors.As(err, &ve) {
		return Error{}, false
	}
	details := make([]ErrorDetail, len(ve.Details))
	for i, d := range ve.Details {
		details[i] = ErrorDetail{Index: d.Index, Field: d.Field, Reason: d.Reason}
	}
	return Error{Code: "invalid_request", Message: ve.Error(), Details: &details}, true
}
