package httpapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func TestABodyOverTheLimitIs413InTheErrorShape(t *testing.T) {
	for _, path := range []string{"/v1/notifications", "/v1/notifications/batch"} {
		t.Run(path, func(t *testing.T) {
			body := `{"to":"a@example.com","channel":"email","content":"` +
				strings.Repeat("a", notification.MaxRequestBodyBytes) + `"}`
			rec := serve(t, nil, http.MethodPost, path, body)
			if rec.Code != http.StatusRequestEntityTooLarge || decodeError(t, rec).Code != "request_too_large" {
				t.Fatalf("got %d %.300s", rec.Code, rec.Body)
			}
		})
	}
}
