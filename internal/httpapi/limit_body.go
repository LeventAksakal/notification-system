package httpapi

import (
	"net/http"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// limitBody caps every request body at notification.MaxRequestBodyBytes, for
// routes outside the contract too. Huma enforces the same limit on each
// operation (MaxBodyBytes) and answers 413 through toError.
func limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && r.Body != http.NoBody {
			r.Body = http.MaxBytesReader(w, r.Body, notification.MaxRequestBodyBytes)
		}
		next.ServeHTTP(w, r)
	})
}
