package delivery

import (
	"context"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/telemetry"
)

// jobContext carries the ids every log line about the job needs.
func jobContext(ctx context.Context, job notification.Job) context.Context {
	return telemetry.WithCorrelationID(telemetry.WithNotificationID(ctx, job.ID), job.CorrelationID)
}
