// Package support starts the real dependencies tests run against: the
// provider double in-process, and (build tag integration) MySQL and Valkey in
// containers. Every helper registers its own cleanup with t.Cleanup.
package support

import (
	"net/http/httptest"
	"testing"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

// Provider starts the provider double on a local port and returns its base
// URL and the server, whose Stats and Requests are what tests assert on.
func Provider(t testing.TB, opts ...fakeprovider.Option) (string, *fakeprovider.Server) {
	t.Helper()
	s := fakeprovider.New(opts...)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts.URL, s
}
