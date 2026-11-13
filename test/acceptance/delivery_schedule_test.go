//go:build integration

package acceptance

import (
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

// B2 (delivery half): Bonus: Scheduled Notifications, allow scheduling
// notifications for future delivery (a notification scheduled for T is not
// sent before T and is sent shortly after; decision 0016).
// Dials: none beyond a running worker; the claim uses the database clock, so
// the "not before T" check stops 200 ms short of T to allow for skew between
// the host and the MySQL container.
func TestDeliveryB2ScheduledDelivery(t *testing.T) {
	const skew = 200 * time.Millisecond
	s := startStack(t, stackOptions{Workers: 1, Env: dlFastEnv(nil)})

	at := time.Now().Add(3 * time.Second).UTC()
	body := func(content string) map[string]any {
		b := msg("push", content)
		b["scheduledAt"] = at.Format(time.RFC3339Nano)
		return b
	}
	due := s.create(t, body("B2 scheduled"))
	dropped := s.create(t, body("B2 scheduled then cancelled"))
	if c := s.cancel(t, dropped.ID); c.Status != "cancelled" {
		t.Fatalf("cancel before T: want cancelled, got %q", c.Status)
	}

	reached := func(id string) int { return len(s.Provider.Requests(fakeprovider.Filter{Key: id})) }
	consistently(t, time.Until(at.Add(-skew)), func() bool {
		return reached(due.ID) == 0 && reached(dropped.ID) == 0
	}, "nothing scheduled reaches the provider before T")

	// Sent within 2 s after T (plus 300 ms for this poll and the HTTP round trip).
	deadline := at.Add(2*time.Second + 300*time.Millisecond)
	dlEventually(t, time.Until(deadline), "the scheduled notification is sent within 2s after T", func() (bool, string) {
		st := s.storedStatus(t, due.ID)
		return st == "sent", "stored status " + st
	})
	rs := s.dlRequests(fakeprovider.Filter{Key: due.ID})
	if len(rs) == 0 {
		t.Fatal("sent, but the double has no request for it")
	}
	if early := at.Sub(rs[0].ReceivedAt); early > skew {
		t.Errorf("reached the provider %s before T", early)
	}
	t.Logf("scheduled for %s, reached the provider %s after T", at.Format(time.RFC3339Nano), rs[0].ReceivedAt.Sub(at).Round(time.Millisecond))

	consistently(t, time.Second, func() bool { return reached(dropped.ID) == 0 }, "the cancelled scheduled notification never reaches the provider")
	if st := s.storedStatus(t, dropped.ID); st != "cancelled" {
		t.Errorf("cancelled scheduled notification: stored status %q", st)
	}
}
