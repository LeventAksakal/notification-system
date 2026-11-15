//go:build integration

package health_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/support"
)

// The script must keep the same rules as MemoryState, on Valkey's clock.
// Time moves for real here, so advance sleeps; the contract's durations are
// kept short for that.
func TestValkeyStateKeepsTheContract(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	var n atomic.Int64
	runStateContract(t, func(t *testing.T) fixture {
		prefix := fmt.Sprintf("health-test-%d:", n.Add(1)) // a fresh state per case
		return fixture{
			state:   health.NewValkey(client, contractCfg, prefix),
			advance: time.Sleep, // Valkey's clock cannot be moved
			wipe: func() health.State {
				if err := client.Do(t.Context(), client.B().Flushall().Build()).Error(); err != nil {
					t.Fatal(err)
				}
				return health.NewValkey(client, contractCfg, prefix)
			},
			restoreOld: func() health.State {
				// As an older snapshot: closed, with a generation of 1 µs.
				key := prefix + string(notification.ChannelSMS)
				if err := client.Do(t.Context(), client.B().Del().Key(key).Build()).Error(); err != nil {
					t.Fatal(err)
				}
				if err := client.Do(t.Context(), client.B().Hset().Key(key).FieldValue().FieldValue("gen", "1").Build()).Error(); err != nil {
					t.Fatal(err)
				}
				return health.NewValkey(client, contractCfg, prefix)
			},
		}
	})
}
