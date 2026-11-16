package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/LeventAksakal/notification-system/internal/intake"
)

// Template versions on the wire (decision 0015, optimistic concurrency): the
// version as a strong ETag, If-Match read back into a number, and the PUT
// operation that creates (201), replaces (200) or refuses (412, 428).

// ifMatchField names the header in a refusal's details.
const ifMatchField = "If-Match"

// ifMatchPattern is one strong ETag holding a version: 1 to 10 digits, the
// digits of 4,294,967,295 (a version is a MySQL INT UNSIGNED).
const ifMatchPattern = `^"[1-9][0-9]{0,9}"$`

// ifMatch is the If-Match header of PUT /v1/templates/{name}; "" when absent.
type ifMatch string

func (ifMatch) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.Pattern = ifMatchPattern
	s.Description = "The template's `ETag` as last read, such as `\"3\"`. Required to replace an existing " +
		"template; absent to create one. One strong ETag only: `*`, weak and listed ETags are refused. " +
		"At most 10 digits, because a version is a MySQL INT UNSIGNED (at most 4294967295)."
	return s
}

// etagHeaderDoc documents the ETag response header.
const etagHeaderDoc = "The template's version as a strong ETag, such as `\"3\"`; send it back in `If-Match`"

// registerPutTemplate adds PUT /v1/templates/{name} to api (called from register).
func (s *Server) registerPutTemplate(api huma.API) {
	op := operation(huma.Operation{
		OperationID: "putTemplate", Method: http.MethodPut, Path: "/v1/templates/{name}", Tags: []string{"templates"},
		Summary: "Create or replace a template",
		Description: "Variables are written `{{name}}`. Replacing a template never changes a notification that\n" +
			"was already accepted: content is rendered at intake.\n\n" +
			"Optimistic concurrency: every template has a `version`, 1 when created and one more on\n" +
			"every update, also sent as a strong `ETag` (`\"3\"`). Without `If-Match` the request\n" +
			"creates the template (`201`); if it already exists the answer is `428` with its current\n" +
			"version. With `If-Match` set to the version last read, the template is replaced (`200`);\n" +
			"any other version, or a template that no longer exists, is `412` and nothing changes.\n",
		RequestBody: jsonBody(map[string]*huma.Example{"welcome": {Value: map[string]any{"body": "Hello {{name}}, welcome aboard."}}}),
	}, "Replaced", 400, 503)
	// Huma documents the default status (200) from the output type; the
	// other success and the two refusals are written here.
	op.Responses[strconv.Itoa(http.StatusCreated)] = &huma.Response{
		Description: "Created",
		Headers:     map[string]*huma.Param{"ETag": {Description: etagHeaderDoc, Schema: &huma.Schema{Type: huma.TypeString}}},
		Content: map[string]*huma.MediaType{
			"application/json": {Schema: &huma.Schema{Ref: "#/components/schemas/Template"}},
		},
	}
	op.Responses[strconv.Itoa(http.StatusPreconditionFailed)] = errorBody("`If-Match` is not the current version, or the template " +
		"no longer exists (`precondition_failed`); the detail's `currentVersion` is the current one when it exists")
	op.Responses[strconv.Itoa(http.StatusPreconditionRequired)] = errorBody("The template exists and replacing it needs " +
		"`If-Match` (`precondition_required`); the detail's `currentVersion` is the current one")
	huma.Register(api, op, s.PutTemplate)
}

type putTemplateInput struct {
	TemplateNameInput
	IfMatch ifMatch `header:"If-Match"`
	Body    TemplateWrite
}

// Resolve: every operation with a body runs singleValues (see refuseNull).
func (*putTemplateInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

type putTemplateOutput struct {
	Status int
	ETag   string `header:"ETag" doc:"The template's version as a strong ETag, such as \"3\"; send it back in If-Match"`
	Body   Template
}

func (s *Server) PutTemplate(ctx context.Context, in *putTemplateInput) (*putTemplateOutput, error) {
	var version *uint64
	if in.IfMatch != "" {
		v, ok := parseETag(string(in.IfMatch))
		if !ok {
			return nil, invalid([]ErrorDetail{{Field: ifMatchField, Reason: `one strong ETag as returned by GET, such as "3"`}})
		}
		version = &v
	}
	t, created, err := s.intake.PutTemplate(ctx, string(in.Name), in.Body.Body, version)
	var ve *intake.VersionError
	if errors.As(err, &ve) {
		return nil, versionRefusal(ve)
	}
	if err := refusal(err); err != nil {
		return nil, err
	}
	out := &putTemplateOutput{Status: http.StatusOK, ETag: etagOf(t.Version), Body: toTemplate(t)}
	if created {
		out.Status = http.StatusCreated
	}
	return out, nil
}

// etagOf is a template version as a strong ETag: the number in quotes.
func etagOf(version uint32) string { return `"` + strconv.FormatUint(uint64(version), 10) + `"` }

// parseETag reads one strong ETag holding a version. Huma's validation of
// ifMatchPattern already refuses anything else; this is the conversion, and
// the guard when the handler is called without it.
func parseETag(s string) (uint64, bool) {
	inner, ok := strings.CutPrefix(s, `"`)
	if !ok {
		return 0, false
	}
	inner, ok = strings.CutSuffix(inner, `"`)
	if !ok || inner == "" || inner[0] == '0' {
		return 0, false
	}
	v, err := strconv.ParseUint(inner, 10, 64)
	return v, err == nil
}

// versionRefusal is the 428 (no If-Match on an existing template) or 412
// (stale or absent template) with the If-Match detail.
func versionRefusal(ve *intake.VersionError) *Error {
	e := newError(http.StatusPreconditionFailed, "precondition_failed", ve.Error())
	if errors.Is(ve, intake.ErrVersionRequired) {
		e = newError(http.StatusPreconditionRequired, "precondition_required", ve.Error())
	}
	e.Details = []ErrorDetail{versionDetail(ve)}
	return e
}

// versionDetail is the If-Match detail of a 412 or 428: why, and the
// template's current version (absent when the template does not exist).
func versionDetail(ve *intake.VersionError) ErrorDetail {
	d := ErrorDetail{Field: ifMatchField, Reason: ve.Error()}
	if ve.Current > 0 {
		v := int(ve.Current)
		d.CurrentVersion = &v
	}
	return d
}
