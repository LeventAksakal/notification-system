package obs

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
)

// plainValues turns attribute values the OTLP log bridge does not know into
// strings before a record reaches it: a value of a named string type
// (core.Channel, core.Status) or one that implements fmt.Stringer. The bridge
// would otherwise export them as "unhandled: (core.Channel) sms". Groups are
// walked; other values pass unchanged. The stdout JSON handler does not need it.
type plainValues struct{ slog.Handler }

func (h plainValues) Handle(ctx context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(plainAttr(a))
		return true
	})
	return h.Handler.Handle(ctx, out)
}

func (h plainValues) WithAttrs(as []slog.Attr) slog.Handler {
	return plainValues{h.Handler.WithAttrs(plainAttrs(as))}
}

func (h plainValues) WithGroup(n string) slog.Handler {
	return plainValues{h.Handler.WithGroup(n)}
}

func plainAttrs(as []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, len(as))
	for i, a := range as {
		out[i] = plainAttr(a)
	}
	return out
}

func plainAttr(a slog.Attr) slog.Attr {
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindGroup:
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(plainAttrs(v.Group())...)}
	case slog.KindAny:
		if s, ok := plainString(v.Any()); ok {
			return slog.String(a.Key, s)
		}
	}
	return slog.Attr{Key: a.Key, Value: v}
}

// plainString is x as a string when its underlying kind is string or it is
// a (non-nil) fmt.Stringer. Errors are left to the bridge, which knows them.
func plainString(x any) (string, bool) {
	if x == nil {
		return "", false
	}
	if _, isErr := x.(error); isErr {
		return "", false
	}
	rv := reflect.ValueOf(x)
	if s, ok := x.(fmt.Stringer); ok {
		if rv.Kind() == reflect.Pointer && rv.IsNil() {
			return "", false
		}
		return s.String(), true
	}
	if rv.Kind() == reflect.String {
		return rv.String(), true
	}
	return "", false
}
