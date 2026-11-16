package delivery

import (
	"context"

	"github.com/LeventAksakal/notification-system/internal/provider"
)

// scriptSender answers by function.
type scriptSender struct {
	fn func(ctx context.Context, m provider.Message) (provider.Response, error)
}

func (s scriptSender) Send(ctx context.Context, m provider.Message) (provider.Response, error) {
	return s.fn(ctx, m)
}

func accept(context.Context, provider.Message) (provider.Response, error) {
	return provider.Response{Status: 202, MessageID: "m"}, nil
}
