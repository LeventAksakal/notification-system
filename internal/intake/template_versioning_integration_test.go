//go:build integration

package intake_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/support"
)

// noEvents drops every event: these tests read rows, not the stream.
type noEvents struct{}

func (noEvents) Publish(context.Context, notification.Event) error { return nil }

// Decision 0015 ("As built"): template versions with optimistic concurrency,
// the template origin on every notification rendered from one, and the
// detailed missing-variables refusal, against real MySQL.
func TestTemplateVersioningAndTraceability(t *testing.T) {
	db, _ := support.MySQL(t)
	svc := intake.New(db, noEvents{}, time.Hour)
	ctx := t.Context()
	v := func(n uint64) *uint64 { return &n }
	put := func(t *testing.T, name, body string, ifMatch *uint64) intake.Template {
		t.Helper()
		tpl, _, err := svc.PutTemplate(ctx, name, body, ifMatch)
		if err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
		return tpl
	}
	wantVersionError := func(t *testing.T, err, why error, current uint32) {
		t.Helper()
		var ve *intake.VersionError
		if !errors.As(err, &ve) || !errors.Is(err, why) || ve.Current != current {
			t.Fatalf("want %v with current version %d, got %v", why, current, err)
		}
	}
	push := func(tpl string, vars map[string]string) intake.Request {
		return intake.Request{To: "device-1", Channel: notification.ChannelPush, Template: &intake.TemplateRef{Name: tpl, Variables: vars}}
	}

	t.Run("create, replace with If-Match, refuse a missing or stale one", func(t *testing.T) {
		tpl, created, err := svc.PutTemplate(ctx, "greet", "Hi {{name}}", nil)
		if err != nil || !created || tpl.Version != 1 {
			t.Fatalf("create: version %d created %v err %v", tpl.Version, created, err)
		}

		_, _, err = svc.PutTemplate(ctx, "greet", "Hello {{name}}", nil)
		wantVersionError(t, err, intake.ErrVersionRequired, 1)

		tpl, created, err = svc.PutTemplate(ctx, "greet", "Hello {{name}}", v(1))
		if err != nil || created || tpl.Version != 2 || tpl.Body != "Hello {{name}}" {
			t.Fatalf("replace: %+v created %v err %v", tpl, created, err)
		}

		_, _, err = svc.PutTemplate(ctx, "greet", "Lost update", v(1))
		wantVersionError(t, err, intake.ErrVersionStale, 2)
		_, _, err = svc.PutTemplate(ctx, "greet", "Out of range", v(1<<32))
		wantVersionError(t, err, intake.ErrVersionStale, 2)
		if got, _ := svc.GetTemplate(ctx, "greet"); got.Body != "Hello {{name}}" || got.Version != 2 {
			t.Fatalf("a refused write changed the template: %+v", got)
		}

		_, _, err = svc.PutTemplate(ctx, "never-made", "x", v(1))
		wantVersionError(t, err, intake.ErrVersionStale, 0)
	})

	t.Run("of concurrent replaces from the same version exactly one wins", func(t *testing.T) {
		put(t, "race", "v1", nil)
		const writers = 8 // arbitrary: enough to collide on one row
		var wg sync.WaitGroup
		errs := make([]error, writers)
		for i := range writers {
			wg.Go(func() { _, _, errs[i] = svc.PutTemplate(ctx, "race", "from a writer", v(1)) })
		}
		wg.Wait()
		won := 0
		for _, err := range errs {
			if err == nil {
				won++
				continue
			}
			wantVersionError(t, err, intake.ErrVersionStale, 2)
		}
		if won != 1 {
			t.Fatalf("%d writers won, want exactly 1", won)
		}
	})

	t.Run("a notification records the template and version it was rendered from", func(t *testing.T) {
		put(t, "trace", "One {{x}}", nil)
		put(t, "trace", "Two {{x}}", v(1))
		n, _, err := svc.Create(ctx, push("trace", map[string]string{"x": "!"}), intake.Meta{CorrelationID: "c"})
		if err != nil {
			t.Fatal(err)
		}
		got, err := svc.Get(ctx, n.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Content != "Two !" || got.TemplateName.String != "trace" || got.TemplateVersion.Int64 != 2 {
			t.Fatalf("content %q origin %v/%v, want Two ! from trace v2", got.Content, got.TemplateName, got.TemplateVersion)
		}

		content := "as is"
		plain, _, err := svc.Create(ctx, intake.Request{To: "device-1", Channel: notification.ChannelPush, Content: &content}, intake.Meta{CorrelationID: "c"})
		if err != nil {
			t.Fatal(err)
		}
		if plain.TemplateName.Valid || plain.TemplateVersion.Valid {
			t.Fatalf("plain content has an origin: %v/%v", plain.TemplateName, plain.TemplateVersion)
		}

		page, err := svc.List(ctx, intake.Filter{Limit: 200})
		if err != nil {
			t.Fatal(err)
		}
		listed := map[string]intake.Notification{}
		for _, it := range page.Items {
			listed[it.ID] = it
		}
		if got := listed[n.ID]; got.TemplateName.String != "trace" || got.TemplateVersion.Int64 != 2 {
			t.Errorf("list: templated item origin %v/%v, want trace v2", got.TemplateName, got.TemplateVersion)
		}
		if got, ok := listed[plain.ID]; !ok || got.TemplateName.Valid || got.TemplateVersion.Valid {
			t.Errorf("list: plain item listed %v with origin %v/%v, want listed with none", ok, got.TemplateName, got.TemplateVersion)
		}
	})

	t.Run("each batch item records its own template origin", func(t *testing.T) {
		put(t, "batch-a", "A {{x}}", nil)
		put(t, "batch-b", "B {{x}}", nil)
		put(t, "batch-b", "B2 {{x}}", v(1))
		content := "plain"
		b, _, err := svc.CreateBatch(ctx, []intake.Request{
			push("batch-a", map[string]string{"x": "1"}),
			{To: "device-1", Channel: notification.ChannelPush, Content: &content},
			push("batch-b", map[string]string{"x": "3"}),
		}, intake.Meta{CorrelationID: "c"})
		if err != nil {
			t.Fatal(err)
		}
		want := []struct {
			name    string
			version int64
		}{{"batch-a", 1}, {"", 0}, {"batch-b", 2}}
		for i, id := range b.NotificationIDs {
			n, err := svc.Get(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if n.TemplateName.String != want[i].name || n.TemplateVersion.Int64 != want[i].version || n.TemplateName.Valid != (want[i].name != "") {
				t.Errorf("item %d: origin %v/%v, want %s v%d", i, n.TemplateName, n.TemplateVersion, want[i].name, want[i].version)
			}
		}
	})

	t.Run("an idempotent replay keeps the first answer after the template changed", func(t *testing.T) {
		put(t, "replay", "First {{x}}", nil)
		meta := intake.Meta{CorrelationID: "c", IdempotencyKey: "replay-after-edit"}
		first, _, err := svc.Create(ctx, push("replay", map[string]string{"x": "!"}), meta)
		if err != nil {
			t.Fatal(err)
		}
		put(t, "replay", "Second {{x}} {{y}}", v(1)) // would now refuse the request: y is missing
		again, replayed, err := svc.Create(ctx, push("replay", map[string]string{"x": "!"}), meta)
		if err != nil || !replayed || again.ID != first.ID || again.TemplateVersion.Int64 != 1 {
			t.Fatalf("replay: replayed %v id %s version %v err %v", replayed, again.ID, again.TemplateVersion, err)
		}
	})

	t.Run("missing variables are refused with required, missing and unknown; unknown alone is accepted", func(t *testing.T) {
		put(t, "vars", "{{b}} then {{a}} then {{b}}", nil)
		_, _, err := svc.Create(ctx, push("vars", map[string]string{"a": "1", "zz": "2", "c": "3"}), intake.Meta{CorrelationID: "c"})
		var ve *intake.ValidationError
		if !errors.As(err, &ve) || len(ve.Details) != 1 || ve.Details[0].Variables == nil {
			t.Fatalf("want one template.variables detail, got %v", err)
		}
		r := ve.Details[0].Variables
		if ve.Details[0].Field != "template.variables" || !slices.Equal(r.Required, []string{"b", "a"}) ||
			!slices.Equal(r.Missing, []string{"b"}) || !slices.Equal(r.Unknown, []string{"c", "zz"}) {
			t.Fatalf("detail = %s %+v", ve.Details[0].Field, r)
		}

		if _, _, err := svc.Create(ctx, push("vars", map[string]string{"a": "1", "b": "2", "zz": "unused"}), intake.Meta{CorrelationID: "c"}); err != nil {
			t.Fatalf("unknown variables alone were refused: %v", err)
		}
	})
}
