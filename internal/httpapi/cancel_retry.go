package httpapi

import (
	"context"
	"errors"

	"github.com/LeventAksakal/notification-system/internal/intake"
)

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
