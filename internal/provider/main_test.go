package provider

import (
	"testing"

	"go.uber.org/goleak"
)

// Every test closes its clients' idle connections (testClient) and its
// servers, so no transport goroutine outlives the package.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
