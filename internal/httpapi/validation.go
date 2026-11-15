package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	middleware "github.com/oapi-codegen/nethttp-middleware"
)

// writeValidationError is the spec validator's error handler. A request the
// contract forbids gets 400 invalid_request with one detail per offending
// field; an unknown route gets 404 not_found.
func writeValidationError(_ context.Context, err error, w http.ResponseWriter, _ *http.Request, opts middleware.ErrorHandlerOpts) {
	switch {
	case tooLarge(err):
		writeTooLarge(w)
		return
	case opts.MatchedRoute == nil:
		writeError(w, opts.StatusCode, Error{Code: "not_found", Message: "no such route"})
		return
	case opts.StatusCode >= http.StatusInternalServerError:
		writeError(w, opts.StatusCode, Error{Code: "internal", Message: "request validation failed"})
		return
	}
	details := validationDetails(err)
	msg := "the request breaks the API contract"
	if len(details) > 0 {
		msg = details[0].Field + ": " + details[0].Reason
		if len(details) > 1 {
			msg += " (and " + strconv.Itoa(len(details)-1) + " more)"
		}
	}
	body := Error{Code: "invalid_request", Message: msg}
	if len(details) > 0 {
		body.Details = &details
	}
	writeError(w, http.StatusBadRequest, body)
}

// validationDetails flattens a kin-openapi validation error into one detail
// per offending field. Body fields are named by their JSON path joined with
// dots; an item of a batch carries its index; parameters by their name.
//
// The outermost type decides, not errors.As: a parameter's RequestError wraps
// a MultiError of SchemaErrors, and unwrapping first would lose its name.
func validationDetails(err error) []ErrorDetail {
	switch e := err.(type) { //nolint:errorlint // the outermost type is the point
	case openapi3.MultiError:
		var out []ErrorDetail
		for _, inner := range e {
			out = append(out, validationDetails(inner)...)
		}
		return out
	case *openapi3filter.RequestError:
		if e.Parameter != nil {
			return []ErrorDetail{{Field: e.Parameter.Name, Reason: requestReason(e)}}
		}
		if e.Err != nil {
			if d := validationDetails(e.Err); len(d) > 0 {
				return d
			}
		}
		return []ErrorDetail{{Field: "body", Reason: requestReason(e)}}
	case *openapi3.SchemaError:
		return []ErrorDetail{schemaDetail(e)}
	}
	if u := errors.Unwrap(err); u != nil {
		return validationDetails(u)
	}
	return nil
}

func schemaDetail(se *openapi3.SchemaError) ErrorDetail {
	path := se.JSONPointer()
	d := ErrorDetail{Reason: se.Reason}
	if len(path) >= 2 && path[0] == "notifications" {
		if i, err := strconv.Atoi(path[1]); err == nil {
			d.Index = &i
			path = path[2:]
		}
	}
	d.Field = strings.Join(path, ".")
	if d.Field == "" {
		if d.Index != nil {
			d.Field = "notifications"
		} else {
			d.Field = "body"
		}
	}
	if d.Reason == "" {
		d.Reason = firstLine(se.Error())
	}
	return d
}

func requestReason(re *openapi3filter.RequestError) string {
	var se *openapi3.SchemaError
	if errors.As(re.Err, &se) && se.Reason != "" {
		return se.Reason
	}
	if re.Reason != "" {
		return re.Reason
	}
	if re.Err != nil {
		return firstLine(re.Err.Error())
	}
	return "invalid"
}

// firstLine drops what follows the first newline: kin-openapi echoes the
// schema and the value there, and the value may be message content.
func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}
