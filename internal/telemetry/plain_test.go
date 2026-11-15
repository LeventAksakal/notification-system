package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// capture keeps the records it gets, with the attributes added by WithAttrs.
type capture struct {
	mu    *sync.Mutex
	recs  *[]slog.Record
	attrs []slog.Attr
}

func (c capture) Enabled(context.Context, slog.Level) bool { return true }
func (c capture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	r = r.Clone()
	r.AddAttrs(c.attrs...)
	*c.recs = append(*c.recs, r)
	return nil
}
func (c capture) WithAttrs(as []slog.Attr) slog.Handler {
	c.attrs = append(append([]slog.Attr(nil), c.attrs...), as...)
	return c
}
func (c capture) WithGroup(string) slog.Handler { return c }

type stringer struct{}

func (stringer) String() string { return "stringer" }

func TestTypedValuesReachTheBridgeAsPlainStrings(t *testing.T) {
	var recs []slog.Record
	log := slog.New(plainValues{capture{mu: &sync.Mutex{}, recs: &recs}})
	log.With("bound", notification.ChannelEmail).InfoContext(context.Background(), "delivery attempt",
		"channel", notification.ChannelSMS, "status", notification.StatusSent, "who", stringer{},
		slog.Group("job", "priority", notification.PriorityHigh), "n", 3, "took", time.Second)
	if len(recs) != 1 {
		t.Fatalf("%d records, want 1", len(recs))
	}
	got := map[string]slog.Value{}
	var walk func(prefix string, as []slog.Attr)
	walk = func(prefix string, as []slog.Attr) {
		for _, a := range as {
			if a.Value.Kind() == slog.KindGroup {
				walk(prefix+a.Key+".", a.Value.Group())
				continue
			}
			got[prefix+a.Key] = a.Value
		}
	}
	var as []slog.Attr
	recs[0].Attrs(func(a slog.Attr) bool { as = append(as, a); return true })
	walk("", as)
	for key, want := range map[string]string{
		"channel": "sms", "status": "sent", "who": "stringer", "job.priority": "high", "bound": "email",
	} {
		if v := got[key]; v.Kind() != slog.KindString || v.String() != want {
			t.Errorf("%s: kind %s value %v, want the string %q", key, v.Kind(), v, want)
		}
	}
	if got["n"].Kind() != slog.KindInt64 || got["took"].Kind() != slog.KindDuration {
		t.Errorf("other kinds must pass unchanged: n %s, took %s", got["n"].Kind(), got["took"].Kind())
	}
}
