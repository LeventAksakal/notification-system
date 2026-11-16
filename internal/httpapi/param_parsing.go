package httpapi

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"uuid"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/validation"
)

// singleValues refuses what Huma lets through for a query or header
// parameter: a single-value one given more than once (Huma reads the first),
// and an empty value where the schema needs at least one character (Huma
// treats it as absent, so an empty Idempotency-Key would create without a
// key).
func singleValues(ctx huma.Context) []error {
	u := ctx.URL()
	q := u.Query()
	h := http.Header{}
	ctx.EachHeader(func(name, value string) { h.Add(name, value) })
	var errs []error
	for _, p := range ctx.Operation().Parameters {
		var vals []string
		switch {
		case p.In == "query" && (p.Schema == nil || p.Schema.Type != huma.TypeArray):
			vals = q[p.Name]
		case p.In == "header":
			vals = h.Values(p.Name)
		default:
			continue
		}
		switch {
		case len(vals) > 1:
			errs = append(errs, &huma.ErrorDetail{Location: p.In + "." + p.Name,
				Message: "expected one value, got " + strconv.Itoa(len(vals))})
		case len(vals) == 1 && vals[0] == "" && p.Schema != nil && p.Schema.MinLength != nil && *p.Schema.MinLength > 0:
			errs = append(errs, &huma.ErrorDetail{Location: p.In + "." + p.Name,
				Message: fmt.Sprintf(validation.MsgExpectedMinLength, *p.Schema.MinLength)})
		}
	}
	if nulls, ok := ctx.Context().Value(nullsKey{}).([]error); ok {
		errs = append(errs, nulls...)
	}
	return errs
}

// uuidParam is a UUID path or query parameter, read by parseUUID.
type uuidParam struct{ uuid.UUID }

func (u *uuidParam) UnmarshalText(b []byte) (err error) {
	u.UUID, err = parseUUID(string(b))
	return err
}

func (uuidParam) Schema(huma.Registry) *huma.Schema {
	return &huma.Schema{Type: huma.TypeString, Format: "uuid"}
}

// parseUUID reads a UUID as Huma's format check accepts it: the standard
// library trims a "urn:uuid:" prefix only in lower case.
func parseUUID(s string) (uuid.UUID, error) {
	if len(s) > 9 && strings.EqualFold(s[:9], "urn:uuid:") {
		s = s[9:]
	}
	return uuid.Parse(s)
}
