package intake

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func TestRequestHashIgnoresDefaultsButNotContent(t *testing.T) {
	at := time.Date(2026, 12, 1, 9, 0, 0, 0, time.FixedZone("x", 3600))
	a := Request{To: "t", Channel: notification.ChannelPush, Content: ptr("x"), ScheduledAt: &at}
	b := a
	b.Priority = notification.PriorityNormal
	utc := at.UTC()
	b.ScheduledAt = &utc
	c := a
	c.Content = ptr("y")
	if string(requestHash("notification", a)) != string(requestHash("notification", b)) {
		t.Error("explicit default priority or another zone changed the hash")
	}
	if string(requestHash("notification", a)) == string(requestHash("notification", c)) {
		t.Error("different content, same hash")
	}
	if string(requestHash("notification", []Request{a})) == string(requestHash("batch", []Request{a})) {
		t.Error("kind is not part of the hash")
	}
}

func TestIdempotencyKeyOutsideVisibleASCIIIsRefusedNamingTheHeader(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		ok        bool
	}{
		{"absent", "", true},
		{"visible ASCII", "order-42:retry_1!~", true},
		{"at its limit", strings.Repeat("k", notification.MaxIdempotencyKeyLen), true},
		{"past its limit", strings.Repeat("k", notification.MaxIdempotencyKeyLen+1), false},
		{"non-ASCII", "clé", false},
		{"space", "a key", false},
		{"control character", "a\tkey", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkIdempotencyKey(tc.key)
			if tc.ok != (err == nil) {
				t.Fatalf("key %q: got %v", tc.key, err)
			}
			if err != nil && err.(*ValidationError).Details[0].Field != "Idempotency-Key" { //nolint:errorlint // concrete type is the contract
				t.Fatalf("key %q: %v does not name the header", tc.key, err)
			}
		})
	}
}

func TestTheRequestHashCoversTheRequestNotTheRenderedContent(t *testing.T) {
	tpl := func(v string) Request {
		return Request{To: "a@example.com", Channel: notification.ChannelEmail,
			Template: &TemplateRef{Name: "welcome", Variables: map[string]string{"name": v}}}
	}
	if !slices.Equal(requestHash("notification", tpl("Ada")), requestHash("notification", tpl("Ada"))) {
		t.Fatal("the same request hashes differently")
	}
	if slices.Equal(requestHash("notification", tpl("Ada")), requestHash("notification", tpl("Bob"))) {
		t.Fatal("different variables hash the same")
	}
	if slices.Equal(requestHash("notification", tpl("Ada")), requestHash("batch", []Request{tpl("Ada")})) {
		t.Fatal("a single create and a batch hash the same")
	}
}
