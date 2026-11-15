package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/validation"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
)

func (e *Error) Error() string { return e.Message }

// GetStatus makes Error a huma.StatusError.
func (e *Error) GetStatus() int { return e.status }

// GetHeaders makes Error a huma.HeadersError (Retry-After on 503).
func (e *Error) GetHeaders() http.Header {
	if len(e.headers) == 0 {
		return nil
	}
	h := make(http.Header, len(e.headers))
	for k, v := range e.headers {
		h.Set(k, v)
	}
	return h
}

func newError(status int, code, msg string) *Error {
	return &Error{status: status, Code: code, Message: msg}
}

func notFound(what string) *Error {
	return newError(http.StatusNotFound, "not_found", "no such "+what)
}

var errKeyReused = newError(http.StatusUnprocessableEntity, "idempotency_key_reused",
	"this Idempotency-Key was used before with a different payload")

// retryAfterSeconds is the Retry-After of a 503 unavailable.
const retryAfterSeconds = 1

// errUnavailable answers 503 unavailable: the database could not be reached,
// or did not answer in time (ServiceUnavailable in the spec). The request may
// or may not have been stored: a connection lost during a commit the server
// did apply looks the same. The client repeats it with the same
// Idempotency-Key.
func errUnavailable() *Error {
	e := newError(http.StatusServiceUnavailable, "unavailable",
		"the database cannot be reached; the request may or may not have been stored; repeat it with the same Idempotency-Key")
	e.headers = map[string]string{"Retry-After": strconv.Itoa(retryAfterSeconds)}
	return e
}

func errInternal() *Error {
	return newError(http.StatusInternalServerError, "internal", "internal error")
}

func errTooLarge() *Error {
	return newError(http.StatusRequestEntityTooLarge, "request_too_large",
		fmt.Sprintf("a request body is at most %d bytes", notification.MaxRequestBodyBytes))
}

// invalid is the 400 invalid_request body: one detail per broken rule, the
// message naming the first.
func invalid(details []ErrorDetail) *Error {
	msg := "the request breaks the API contract"
	if len(details) > 0 {
		msg = details[0].Field + ": " + details[0].Reason
		if len(details) > 1 {
			msg += " (and " + strconv.Itoa(len(details)-1) + " more)"
		}
	}
	e := newError(http.StatusBadRequest, "invalid_request", msg)
	e.Details = details
	return e
}

// hookErrors makes Huma write the contract's Error body instead of RFC 9457
// problem details. Huma builds every error it writes itself (validation,
// body reading, a handler's plain error) through NewErrorWithContext; the
// handlers return *Error directly, which Huma writes as is.
//
// Huma answers a broken rule with 422 and an unknown content type with 415;
// the contract answers both 400 invalid_request, keeping 422 for a reused
// Idempotency-Key alone.
func hookErrors() {
	huma.NewError = func(status int, msg string, errs ...error) huma.StatusError {
		return toError(nil, status, msg, errs)
	}
	huma.NewErrorWithContext = func(ctx huma.Context, status int, msg string, errs ...error) huma.StatusError {
		return toError(ctx, status, msg, errs)
	}
}

func toError(ctx huma.Context, status int, msg string, errs []error) *Error {
	switch {
	case status == 0: // the example error Huma builds to document the schema
		return &Error{}
	case status == http.StatusRequestEntityTooLarge:
		return errTooLarge()
	case status == http.StatusBadRequest, status == http.StatusUnprocessableEntity, status == http.StatusUnsupportedMediaType:
		var details []ErrorDetail
		for _, err := range errs {
			if d := toDetail(err); d != nil {
				details = append(details, *d)
			}
		}
		if len(details) == 0 && msg == "request body is required" {
			details = []ErrorDetail{{Field: "body", Reason: "value is required but missing"}}
		}
		return invalid(details)
	case status >= http.StatusInternalServerError:
		cause := errors.Join(errs...)
		if store.IsUnavailable(cause) {
			// Logged once here, at WARN: the outage is an operating
			// condition, and the readiness probe reports it too. Driver
			// errors carry the address, never the DSN's credentials.
			logRequest(ctx, slog.LevelWarn, "database unreachable; answered 503", cause)
			e := errUnavailable()
			// Huma applies the headers of the handler's own error only, not
			// of the one built here.
			for k, v := range e.headers {
				if ctx != nil {
					ctx.SetHeader(k, v)
				}
			}
			return e
		}
		logRequest(ctx, slog.LevelError, "request failed: "+msg, cause)
		return errInternal()
	}
	return newError(status, strings.ReplaceAll(strings.ToLower(http.StatusText(status)), " ", "_"), msg)
}

func logRequest(ctx huma.Context, level slog.Level, msg string, err error) {
	if ctx == nil {
		slog.Log(context.Background(), level, msg, "err", err)
		return
	}
	slog.Log(ctx.Context(), level, msg, "method", ctx.Method(), "path", ctx.URL().Path, "err", err)
}

// toDetail turns one Huma validation error into the contract's detail.
// Huma locates an error by path ("body.notifications[3].to",
// "query.limit", "header.Idempotency-Key"); the contract names the field
// relative to the body or the parameter's name, and a batch item by its
// index. A missing property is named itself, and an unexpected one by its
// parent object, as the spec-first validator did.
func toDetail(err error) *ErrorDetail {
	var ed huma.ErrorDetailer
	if !errors.As(err, &ed) {
		return &ErrorDetail{Field: "body", Reason: firstLine(err.Error())}
	}
	hd := ed.ErrorDetail()
	field, index := fieldOf(hd.Location)
	reason := hd.Message
	if name, ok := requiredProperty(reason); ok {
		field = join(field, name)
		reason = `property "` + name + `" is missing`
	} else if reason == validation.MsgUnexpectedProperty {
		parent, name := unexpected(field, hd.Value)
		field, reason = parent, `property "`+name+`" is unsupported`
	}
	if field == "" {
		field = "body"
		if index != nil {
			field = "notifications"
		}
	}
	return &ErrorDetail{Field: field, Index: index, Reason: firstLine(reason)}
}

// fieldOf splits a Huma location into the contract's field and batch index.
func fieldOf(loc string) (string, *int) {
	for _, in := range []string{"path.", "query.", "header.", "cookie."} {
		if name, ok := strings.CutPrefix(loc, in); ok {
			return name, nil
		}
	}
	rest, ok := strings.CutPrefix(loc, "body")
	if !ok {
		return loc, nil
	}
	rest = strings.TrimPrefix(rest, ".")
	if item, ok := strings.CutPrefix(rest, "notifications["); ok {
		if i, after, ok := strings.Cut(item, "]"); ok {
			if n, err := strconv.Atoi(i); err == nil {
				return strings.TrimPrefix(after, "."), &n
			}
		}
	}
	return rest, nil
}

// requiredProperty reads the property name out of Huma's "required" message.
func requiredProperty(msg string) (string, bool) {
	prefix, suffix, _ := strings.Cut(validation.MsgExpectedRequiredProperty, "%s")
	name, ok := strings.CutPrefix(msg, prefix)
	if !ok {
		return "", false
	}
	return strings.CutSuffix(name, suffix)
}

func join(parent, name string) string {
	if parent == "" || parent == "body" {
		return name
	}
	return parent + "." + name
}

// unexpected splits the location of an unexpected property into its parent
// and its name. Huma joins them with a dot, so a name that holds a dot is
// found among the keys of the object Huma reports (obj).
func unexpected(field string, obj any) (parent, name string) {
	parent, name = cutLast(field)
	if m, ok := obj.(map[string]any); ok {
		for k := range m {
			if (field == k || strings.HasSuffix(field, "."+k)) && len(k) > len(name) {
				parent, name = strings.TrimSuffix(strings.TrimSuffix(field, k), "."), k
			}
		}
	}
	return parent, name
}

func cutLast(field string) (parent, name string) {
	if i := strings.LastIndexByte(field, '.'); i >= 0 {
		return field[:i], field[i+1:]
	}
	return "", field
}

// firstLine drops what follows the first newline; a value echoed there may
// be message content.
func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}
