package wake

import (
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func TestHintsForOneChannelAndPriorityCollapseToTheEarliest(t *testing.T) {
	t0 := time.Unix(1_800_000_000, 0)
	got := coalesce([]notification.Wake{
		{Channel: notification.ChannelSMS, Priority: notification.PriorityLow, Due: t0.Add(2 * time.Second)},
		{Channel: notification.ChannelSMS, Priority: notification.PriorityLow, Due: t0},
		{Channel: notification.ChannelEmail, Priority: notification.PriorityLow, Due: t0.Add(time.Second)},
		{Channel: notification.ChannelSMS, Priority: notification.PriorityHigh, Due: t0.Add(time.Second)},
	})
	want := []notification.Wake{
		{Channel: notification.ChannelSMS, Priority: notification.PriorityLow, Due: t0},
		{Channel: notification.ChannelEmail, Priority: notification.PriorityLow, Due: t0.Add(time.Second)},
		{Channel: notification.ChannelSMS, Priority: notification.PriorityHigh, Due: t0.Add(time.Second)},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("hint %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestPublisherHoldsOffAfterValkeyStopsAnswering(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	p := &Publisher{now: func() time.Time { return now }}
	if _, ok := p.admit(); !ok {
		t.Fatal("a healthy publisher must admit")
	}
	p.settle(t.Context(), false, errDown)
	probe, ok := p.admit()
	if !ok || !probe {
		t.Fatalf("first call during the hold-off: want the one probe, got probe=%v ok=%v", probe, ok)
	}
	if _, ok := p.admit(); ok {
		t.Fatal("a second call while the probe runs must fail at once")
	}
	p.settle(t.Context(), true, errDown)
	now = now.Add(publishHoldOff)
	if probe, ok := p.admit(); !ok || probe {
		t.Fatalf("after the hold-off: want an ordinary call, got probe=%v ok=%v", probe, ok)
	}
	p.settle(t.Context(), false, nil)
	if p.down {
		t.Fatal("a success must end the hold-off")
	}
}

var errDown = errTimeout{}

type errTimeout struct{}

func (errTimeout) Error() string   { return "i/o timeout" }
func (errTimeout) Timeout() bool   { return true }
func (errTimeout) Temporary() bool { return true }
