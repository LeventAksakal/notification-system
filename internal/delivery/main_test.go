package delivery

import (
	"testing"

	"go.uber.org/goleak"
)

// The Docker client of the integration tests (testcontainers on Windows)
// starts one process-wide IO completion goroutine that never ends, and the
// MySQL server shared by the process (test/support) keeps its Ryuk
// connection until the process exits.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/Microsoft/go-winio.ioCompletionProcessor"),
		goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"))
}
