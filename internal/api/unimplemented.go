package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// ErrNotImplemented marks an operation whose slice has not landed; the strict
// handler's error func turns it into 501 not_implemented. A slice that builds
// an operation deletes its method here and implements it in its own file.
var ErrNotImplemented = errors.New("not implemented")

func notImplemented(op string) error { return fmt.Errorf("%s: %w", op, ErrNotImplemented) }

func (s *Server) CreateNotification(context.Context, CreateNotificationRequestObject) (CreateNotificationResponseObject, error) {
	return nil, notImplemented("createNotification")
}

func (s *Server) ListNotifications(context.Context, ListNotificationsRequestObject) (ListNotificationsResponseObject, error) {
	return nil, notImplemented("listNotifications")
}

func (s *Server) CreateBatch(context.Context, CreateBatchRequestObject) (CreateBatchResponseObject, error) {
	return nil, notImplemented("createBatch")
}

func (s *Server) GetNotification(context.Context, GetNotificationRequestObject) (GetNotificationResponseObject, error) {
	return nil, notImplemented("getNotification")
}

func (s *Server) CancelNotification(context.Context, CancelNotificationRequestObject) (CancelNotificationResponseObject, error) {
	return nil, notImplemented("cancelNotification")
}

func (s *Server) RetryNotification(context.Context, RetryNotificationRequestObject) (RetryNotificationResponseObject, error) {
	return nil, notImplemented("retryNotification")
}

func (s *Server) GetBatch(context.Context, GetBatchRequestObject) (GetBatchResponseObject, error) {
	return nil, notImplemented("getBatch")
}

func (s *Server) ListTemplates(context.Context, ListTemplatesRequestObject) (ListTemplatesResponseObject, error) {
	return nil, notImplemented("listTemplates")
}

func (s *Server) PutTemplate(context.Context, PutTemplateRequestObject) (PutTemplateResponseObject, error) {
	return nil, notImplemented("putTemplate")
}

func (s *Server) GetTemplate(context.Context, GetTemplateRequestObject) (GetTemplateResponseObject, error) {
	return nil, notImplemented("getTemplate")
}

func (s *Server) DeleteTemplate(context.Context, DeleteTemplateRequestObject) (DeleteTemplateResponseObject, error) {
	return nil, notImplemented("deleteTemplate")
}

func (s *Server) GetMetrics(context.Context, GetMetricsRequestObject) (GetMetricsResponseObject, error) {
	return nil, notImplemented("getMetrics")
}

// streamEvents is GET /v1/ws (operation streamEvents). It is excluded from the
// generated server (a WebSocket upgrade does not fit the strict interface) and
// mounted outside the validated routes in Handler.
func (s *Server) streamEvents(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, Error{Code: "not_implemented", Message: notImplemented("streamEvents").Error()})
}
