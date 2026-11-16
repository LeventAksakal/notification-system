package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/LeventAksakal/notification-system/internal/intake"
)

// PATCH /v1/notifications/{id}: a new schedule for a notification no worker
// has picked up (decision 0016, "As built").

// NotificationReschedule is the body of PATCH /v1/notifications/{id}.
// scheduledAt is required and nullable: null means due now.
type NotificationReschedule struct {
	ScheduledAt *time.Time `json:"scheduledAt" nullable:"true"`
}

func (NotificationReschedule) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.Properties["scheduledAt"].Description = scheduleDoc("The new schedule; `null` means due now.")
	return s
}

// registerReschedule adds the operation to api (called from register).
func (s *Server) registerReschedule(api huma.API) {
	huma.Register(api, operation(huma.Operation{
		OperationID: "rescheduleNotification", Method: http.MethodPatch, Path: "/v1/notifications/{id}", Tags: []string{"notifications"},
		Summary: "Reschedule a notification that no worker has picked up",
		Description: "Sets `scheduledAt` (and so `dueAt`) under the same rules as create. Allowed while the\n" +
			"notification is pending and no worker has picked it up: `scheduled`, or `pending` and due but not\n" +
			"yet claimed. One a worker already picked up is refused (409; the message names the status): one\n" +
			"waiting for a retry, so a reschedule never overrides a retry backoff or a provider's Retry-After, and\n" +
			"one handed back unsent while its channel was paused, which shows as `scheduled` until it is due. A\n" +
			"reschedule racing a worker has exactly one winner. No WebSocket event is sent: the stored status does\n" +
			"not change.\n",
		RequestBody: jsonBody(map[string]*huma.Example{
			"later": {Value: map[string]any{"scheduledAt": "2026-12-01T09:00:00Z"}},
			"now":   {Value: map[string]any{"scheduledAt": nil}},
		}),
	}, "Rescheduled; the notification as stored now", 404, 409, 400, 503), s.RescheduleNotification)
}

type rescheduleInput struct {
	ID   uuidParam `path:"id"`
	Body NotificationReschedule
}

// Resolve passes on the nulls refuseNull found; scheduledAt itself may be null.
func (*rescheduleInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

func (s *Server) RescheduleNotification(ctx context.Context, in *rescheduleInput) (*notificationOutput, error) {
	n, err := s.intake.Reschedule(ctx, in.ID.String(), in.Body.ScheduledAt)
	switch {
	case errors.Is(err, intake.ErrNotFound):
		return nil, notFound("notification")
	case errors.Is(err, intake.ErrNotReschedulable):
		return nil, newError(409, "not_reschedulable", err.Error())
	}
	if err := refusal(err); err != nil {
		return nil, err
	}
	return &notificationOutput{toNotification(n)}, nil
}
