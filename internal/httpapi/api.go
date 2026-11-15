package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"mime"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// Version is the contract's version (info.version). Bump the minor version
// for an addition, the major one for a breaking change; CI's oasdiff check
// compares the generated spec with the base branch.
const Version = "1.3.0"

const description = `Accepts notification requests (sms, email, push), queues them, and delivers them to an
external provider with rate limiting, priorities, retries and status tracking.

This document is generated from the server's code (internal/httpapi); the server validates
requests against the same schemas.

Conventions
- Every refused request has the ` + "`Error`" + ` body. ` + "`details`" + ` names each offending field
  (and the item index in a batch).
- ` + "`400 invalid_request`" + `: the body or a parameter breaks a rule (missing field, over a limit,
  unknown value). ` + "`404 not_found`. `409`" + `: the resource is not in a state that allows the
  action. ` + "`422 idempotency_key_reused`" + `: the key was used before with a different payload.
  ` + "`503 unavailable` with `Retry-After`" + `: the database cannot be reached or did not answer in
  time. A create may or may not have been stored: repeat it with the same ` + "`Idempotency-Key`" + `
  and it is stored once.
- ` + "`X-Correlation-ID`" + ` may be sent on any request; one is generated when absent. It is echoed
  on the response, stored on the notification, written on every log line about it and sent
  to the provider.
- ` + "`Idempotency-Key`" + ` on a create request stores the request once. A repeat with the same key
  and the same payload returns the first response (same ids) with ` + "`Idempotent-Replayed: true`" + `.
- Limits (single source: ` + "`internal/notification/limits.go`" + `): sms ` + "`to`" + ` is E.164, content 1..1600
  characters; email ` + "`to`" + ` is an address of at most 254 characters, content 1..100000; push
  ` + "`to`" + ` is a device token of 1..255 characters, content 1..1024. Length is counted in
  characters, not bytes. A batch holds 1..1000 notifications. A request body is at most
  8 MiB (` + "`413 request_too_large`" + `).
`

func init() {
	hookErrors()
	// The contract's arrays are never null: handlers always set them.
	huma.DefaultArrayNullable = false
	// JSON field names are case-sensitive: {"TO": ...} is an unknown field,
	// not `to` (Huma matches them case-insensitively by default).
	huma.ValidateStrictCasing = true
}

// config is the Huma configuration: JSON only, no $schema links in bodies,
// and no built-in /openapi or /docs routes (Handler serves the 3.0 document
// at /openapi.json and the vendored Scalar page at /docs).
func config() huma.Config {
	c := huma.DefaultConfig("Notification system", Version)
	c.Info.Description = description
	c.Servers = []*huma.Server{{URL: "http://localhost:8080"}}
	c.Tags = []*huma.Tag{{Name: "notifications"}, {Name: "batches"}, {Name: "templates"}, {Name: "operations"}}
	c.Formats = map[string]huma.Format{"application/json": huma.DefaultJSONFormat, "json": huma.DefaultJSONFormat}
	c.CreateHooks = nil
	c.OpenAPIPath, c.DocsPath, c.SchemasPath = "", "", ""
	c.Components.Responses = responses()
	// Every error response an operation documents is named in its Responses
	// (refs to the components below); Huma's catch-all "default" goes.
	c.OnAddOperation = append(c.OnAddOperation, func(_ *huma.OpenAPI, op *huma.Operation) {
		delete(op.Responses, "default")
	})
	return c
}

func errorBody(description string) *huma.Response {
	return &huma.Response{Description: description, Content: map[string]*huma.MediaType{
		"application/json": {Schema: &huma.Schema{Ref: "#/components/schemas/Error"}},
	}}
}

func responses() map[string]*huma.Response {
	unavailable := errorBody("The database cannot be reached or did not answer in time (`unavailable`). " +
		"A change may or may not have been made; repeat a create with the same Idempotency-Key")
	unavailable.Headers = map[string]*huma.Param{"Retry-After": {
		Description: "Seconds to wait before trying again",
		Schema:      &huma.Schema{Type: huma.TypeInteger},
	}}
	return map[string]*huma.Response{
		"InvalidRequest":       errorBody("The request breaks a rule; `details` names each field"),
		"NotFound":             errorBody("No such resource"),
		"Conflict":             errorBody("The resource is not in a state that allows this action"),
		"RequestTooLarge":      errorBody("The request body is larger than 8 MiB (`request_too_large`)"),
		"IdempotencyKeyReused": errorBody("The Idempotency-Key was used before with a different payload"),
		"ServiceUnavailable":   unavailable,
	}
}

var responseNames = map[int]string{
	http.StatusBadRequest:            "InvalidRequest",
	http.StatusNotFound:              "NotFound",
	http.StatusConflict:              "Conflict",
	http.StatusRequestEntityTooLarge: "RequestTooLarge",
	http.StatusUnprocessableEntity:   "IdempotencyKeyReused",
	http.StatusServiceUnavailable:    "ServiceUnavailable",
}

// operation fills what every contract operation shares: the success
// response's description, the documented error responses, the body limit,
// and no body read deadline of Huma's own (the request deadline applies).
func operation(op huma.Operation, success string, errs ...int) huma.Operation {
	op.Responses = map[string]*huma.Response{}
	status := op.DefaultStatus
	if status == 0 {
		status = http.StatusOK
	}
	op.Responses[strconv.Itoa(status)] = &huma.Response{Description: success}
	for _, code := range errs {
		op.Responses[strconv.Itoa(code)] = &huma.Response{Ref: "#/components/responses/" + responseNames[code]}
	}
	op.MaxBodyBytes = notification.MaxRequestBodyBytes
	op.BodyReadTimeout = -1
	return op
}

// Parameter shapes. Each carries its limits from internal/notification.

// idempotencyKey is the Idempotency-Key header: visible ASCII, no spaces.
type idempotencyKey string

func (idempotencyKey) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.MinLength, s.MaxLength, s.Pattern = ptr(1), ptr(notification.MaxIdempotencyKeyLen), "^[!-~]+$"
	s.Description = "Visible ASCII, no spaces"
	return s
}

// correlationID is the X-Correlation-ID header.
type correlationID string

func (correlationID) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.MinLength, s.MaxLength, s.Pattern = ptr(1), ptr(notification.MaxCorrelationIDLen), correlationPattern
	return s
}

// pageLimit is `limit` on list endpoints; 0 (absent) means the default.
type pageLimit int

func (pageLimit) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.Minimum, s.Maximum, s.Default = ptr(1.0), ptr(float64(notification.MaxPageSize)), notification.DefaultPageSize
	return s
}

// cursor is the opaque `nextCursor` of the previous page.
type cursor string

func (cursor) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.MinLength, s.MaxLength = ptr(1), ptr(notification.MaxCursorLen)
	s.Description = "Opaque; the `nextCursor` of the previous page"
	return s
}

// windowSeconds is GetMetrics' window; 0 (absent) means the default.
type windowSeconds int

func (windowSeconds) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.Minimum, s.Maximum = ptr(1.0), ptr(notification.MaxMetricsWindow.Seconds())
	s.Default = int(notification.DefaultMetricsWindow / time.Second)
	s.Description = "How far back `delivery` looks"
	return s
}

// register puts every contract operation on api. The handlers are s's
// methods; nothing is called until a request arrives, so a zero Server is
// enough to build the spec.
func (s *Server) register(api huma.API) {
	api.UseMiddleware(requireJSON(api), refuseNull(api))

	huma.Register(api, operation(huma.Operation{
		OperationID: "health", Method: http.MethodGet, Path: "/healthz", Tags: []string{"operations"},
		Summary: "Liveness - the process is up",
	}, "Alive"), s.Health)
	readyOp := operation(huma.Operation{
		OperationID: "ready", Method: http.MethodGet, Path: "/readyz", Tags: []string{"operations"},
		Summary: "Readiness - MySQL and Valkey answer",
	}, "Ready")
	readyOp.Responses["503"] = &huma.Response{Description: "A dependency is down", Content: map[string]*huma.MediaType{
		"application/json": {Schema: &huma.Schema{Ref: "#/components/schemas/Health"}},
	}}
	huma.Register(api, readyOp, s.Ready)

	created := operation(huma.Operation{
		OperationID: "createNotification", Method: http.MethodPost, Path: "/v1/notifications", Tags: []string{"notifications"},
		Summary: "Queue one notification", DefaultStatus: http.StatusCreated,
		Description: "Returns as soon as the notification is stored; delivery happens in the workers.",
		RequestBody: jsonBody(map[string]*huma.Example{
			"sms":      {Value: map[string]any{"to": "+905551234567", "channel": "sms", "content": "Your code is 4821", "priority": "high"}},
			"template": {Value: map[string]any{"to": "ada@example.com", "channel": "email", "template": map[string]any{"name": "welcome", "variables": map[string]any{"name": "Ada"}}}},
			"scheduled": {Value: map[string]any{"to": "device-token-1", "channel": "push", "content": "Sale starts now",
				"scheduledAt": "2026-12-01T09:00:00Z"}},
		}),
	}, "Stored and queued", 400, 413, 422, 503)
	huma.Register(api, created, s.CreateNotification)
	huma.Register(api, operation(huma.Operation{
		OperationID: "listNotifications", Method: http.MethodGet, Path: "/v1/notifications", Tags: []string{"notifications"},
		Summary: "List notifications, newest first",
		Description: "Keyset pagination: pass `nextCursor` back as `cursor`. Pages never repeat or skip a row\n" +
			"while new notifications arrive. Dead letters are `status=failed`.\n",
	}, "One page", 400, 503), s.ListNotifications)
	huma.Register(api, operation(huma.Operation{
		OperationID: "createBatch", Method: http.MethodPost, Path: "/v1/notifications/batch", Tags: []string{"batches"},
		Summary: "Queue up to 1000 notifications atomically", DefaultStatus: http.StatusCreated,
		Description: "Every item is validated first; then all are stored with the batch, or nothing is.",
	}, "All stored and queued", 400, 413, 422, 503), s.CreateBatch)
	huma.Register(api, operation(huma.Operation{
		OperationID: "getNotification", Method: http.MethodGet, Path: "/v1/notifications/{id}", Tags: []string{"notifications"},
		Summary: "Status, attempt count, last error and the attempt history of one notification",
	}, "Found; `deliveryAttempts` is included", 404, 400, 503), s.GetNotification)
	huma.Register(api, operation(huma.Operation{
		OperationID: "cancelNotification", Method: http.MethodPost, Path: "/v1/notifications/{id}/cancel", Tags: []string{"notifications"},
		Summary:     "Cancel a notification that has not been picked up",
		Description: "Allowed from `pending` only (including scheduled and waiting to retry). A cancel racing a worker has exactly one winner.",
	}, "Cancelled; it will never reach the provider", 404, 409, 400, 503), s.CancelNotification)
	huma.Register(api, operation(huma.Operation{
		OperationID: "retryNotification", Method: http.MethodPost, Path: "/v1/notifications/{id}/retry", Tags: []string{"notifications"},
		Summary:     "Replay a dead letter",
		Description: "Allowed from `failed` only. The notification returns to `pending` with a fresh attempt budget; its attempt history is kept.",
	}, "Queued again", 404, 409, 400, 503), s.RetryNotification)
	huma.Register(api, operation(huma.Operation{
		OperationID: "getBatch", Method: http.MethodGet, Path: "/v1/batches/{id}", Tags: []string{"batches"},
		Summary: "Counts per status and the items of one batch",
	}, "Found", 400, 404, 503), s.GetBatch)
	huma.Register(api, operation(huma.Operation{
		OperationID: "listTemplates", Method: http.MethodGet, Path: "/v1/templates", Tags: []string{"templates"},
		Summary: "All templates, by name",
	}, "Templates", 503), s.ListTemplates)
	huma.Register(api, operation(huma.Operation{
		OperationID: "putTemplate", Method: http.MethodPut, Path: "/v1/templates/{name}", Tags: []string{"templates"},
		Summary: "Create or replace a template",
		Description: "Variables are written `{{name}}`. Replacing a template never changes a notification that\n" +
			"was already accepted: content is rendered at intake.\n",
		RequestBody: jsonBody(map[string]*huma.Example{"welcome": {Value: map[string]any{"body": "Hello {{name}}, welcome aboard."}}}),
	}, "Stored", 400, 503), s.PutTemplate)
	huma.Register(api, operation(huma.Operation{
		OperationID: "getTemplate", Method: http.MethodGet, Path: "/v1/templates/{name}", Tags: []string{"templates"},
		Summary: "Read one template",
	}, "Found", 404, 400, 503), s.GetTemplate)
	huma.Register(api, operation(huma.Operation{
		OperationID: "deleteTemplate", Method: http.MethodDelete, Path: "/v1/templates/{name}", Tags: []string{"templates"},
		Summary: "Delete a template", DefaultStatus: http.StatusNoContent,
	}, "Deleted", 404, 400, 503), s.DeleteTemplate)
	huma.Register(api, operation(huma.Operation{
		OperationID: "getMetrics", Method: http.MethodGet, Path: "/v1/metrics", Tags: []string{"operations"},
		Summary:     "Queue depth, success and failure rates, latency",
		Description: "Computed from MySQL on every call, so every API instance returns the same numbers.",
	}, "Current numbers", 400, 503), s.GetMetrics)

	documentStream(api)
}

func jsonBody(examples map[string]*huma.Example) *huma.RequestBody {
	return &huma.RequestBody{Content: map[string]*huma.MediaType{"application/json": {Examples: examples}}}
}

// documentStream adds GET /v1/ws to the spec. The route is served outside
// Huma (a WebSocket upgrade, ws.go); its message type, StatusEvent, is the
// Go type the stream writes.
func documentStream(api huma.API) {
	oapi := api.OpenAPI()
	oapi.Components.Schemas.Schema(reflect.TypeFor[StatusEvent](), true, "")
	uuidParam := func(name string) *huma.Param {
		return &huma.Param{Name: name, In: "query", Schema: &huma.Schema{Type: huma.TypeString, Format: "uuid"}}
	}
	op := operation(huma.Operation{
		OperationID: "streamEvents", Method: http.MethodGet, Path: "/v1/ws", Tags: []string{"operations"},
		Summary: "WebSocket stream of status changes",
		Description: "Upgrades to a WebSocket. Each text message is one `StatusEvent` as JSON. With\n" +
			"`notificationId`, the first message is a snapshot of that notification's current status\n" +
			"(`snapshot: true`), then its changes follow in order. With `batchId`, changes of that\n" +
			"batch's notifications follow. With neither, every change follows. The REST API stays the\n" +
			"source of truth. The server closes the stream rather than deliver a stream with holes: a slow\n" +
			"subscriber is disconnected, and so is every subscriber when the server loses its event feed\n" +
			"(reconnect and read the snapshot).\n",
		Parameters: []*huma.Param{uuidParam("notificationId"), uuidParam("batchId")},
	}, "", 400, 404, 503)
	delete(op.Responses, "200")
	op.Responses["101"] = &huma.Response{Description: "Switching protocols; `StatusEvent` messages follow"}
	// The reference keeps StatusEvent in the document (Huma drops schemas
	// nothing refers to) and names it as this operation's message.
	op.Extensions = map[string]any{"x-websocket-message": map[string]any{"$ref": "#/components/schemas/StatusEvent"}}
	oapi.AddOperation(&op)
}

// openResponses lets response objects grow: Huma closes every struct
// (additionalProperties: false), which the request validator needs to refuse
// unknown fields, but on a response it would make every added field a
// breaking change for strict clients. Schemas a request body reaches stay
// closed; the others say nothing, as since 1.0.
func openResponses(oapi *huma.OpenAPI) {
	reg := oapi.Components.Schemas
	request := map[*huma.Schema]bool{}
	var mark func(s *huma.Schema)
	mark = func(s *huma.Schema) {
		if s == nil {
			return
		}
		if s.Ref != "" {
			s = reg.SchemaFromRef(s.Ref)
		}
		if s == nil || request[s] {
			return
		}
		request[s] = true
		mark(s.Items)
		for _, p := range s.Properties {
			mark(p)
		}
	}
	for _, item := range oapi.Paths {
		for _, op := range []*huma.Operation{item.Put, item.Post} {
			if op != nil && op.RequestBody != nil {
				for _, mt := range op.RequestBody.Content {
					mark(mt.Schema)
				}
			}
		}
	}
	for _, s := range reg.Map() {
		if s.Type == huma.TypeObject && !request[s] && s.AdditionalProperties == false {
			s.AdditionalProperties = nil
		}
	}
}

// plainIntegers drops the int32/int64 formats Huma gives Go integers: the
// contract has said `type: integer` since 1.0, and a format would change the
// types generated clients use (int64 instead of int).
func plainIntegers(oapi *huma.OpenAPI) {
	seen := map[*huma.Schema]bool{}
	var walk func(s *huma.Schema)
	walk = func(s *huma.Schema) {
		if s == nil || seen[s] {
			return
		}
		seen[s] = true
		if s.Type == huma.TypeInteger && (s.Format == "int64" || s.Format == "int32") {
			s.Format = ""
		}
		walk(s.Items)
		for _, p := range s.Properties {
			walk(p)
		}
		if ap, ok := s.AdditionalProperties.(*huma.Schema); ok {
			walk(ap)
		}
	}
	for _, s := range oapi.Components.Schemas.Map() {
		walk(s)
	}
	for _, item := range oapi.Paths {
		for _, op := range []*huma.Operation{item.Get, item.Put, item.Post, item.Delete} {
			if op == nil {
				continue
			}
			for _, p := range op.Parameters {
				walk(p.Schema)
			}
			for _, r := range op.Responses {
				for _, h := range r.Headers {
					walk(h.Schema)
				}
				for _, mt := range r.Content {
					walk(mt.Schema)
				}
			}
		}
	}
}

// requireJSON refuses a body that is not declared application/json, as the
// spec-first validator did; Huma would otherwise read a body without a
// Content-Type as JSON.
func requireJSON(api huma.API) func(huma.Context, func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		if ctx.Operation().RequestBody == nil {
			next(ctx)
			return
		}
		ct := ctx.Header("Content-Type")
		if mt, _, err := mime.ParseMediaType(ct); err != nil || mt != "application/json" {
			_ = huma.WriteErr(api, ctx, http.StatusBadRequest, "content type",
				&huma.ErrorDetail{Location: "body", Message: `header Content-Type has unexpected value "` + ct + `"`})
			return
		}
		next(ctx)
	}
}

// refuseNull finds null for a request property the contract does not
// declare nullable: Huma skips validating an optional property set to null,
// while the published schema (and the server before Huma) refuses it. The
// details ride in the context to singleValues, which every operation with a
// body runs as its resolver, so they are answered in one 400 with Huma's own.
// The body is read once here and handed on to Huma; a body without the token
// "null" is not parsed twice.
func refuseNull(api huma.API) func(huma.Context, func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		rb := ctx.Operation().RequestBody
		if rb == nil || rb.Content["application/json"] == nil {
			next(ctx)
			return
		}
		r, _ := humago.Unwrap(ctx)
		b, err := io.ReadAll(io.LimitReader(r.Body, notification.MaxRequestBodyBytes+1))
		r.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(b), r.Body), r.Body}
		var v any
		if err != nil || len(b) > notification.MaxRequestBodyBytes || !bytes.Contains(b, []byte("null")) ||
			json.Unmarshal(b, &v) != nil {
			next(ctx) // Huma answers the rest: too large, malformed, valid
			return
		}
		var errs []error
		nulls(api.OpenAPI().Components.Schemas, rb.Content["application/json"].Schema, v, "body", &errs)
		if len(errs) > 0 {
			ctx = huma.WithValue(ctx, nullsKey{}, errs)
		}
		next(ctx)
	}
}

type nullsKey struct{}

// nulls collects every optional, non-nullable property of v set to null.
// Required properties and array items are left to Huma, which refuses null
// there itself.
func nulls(reg huma.Registry, s *huma.Schema, v any, path string, errs *[]error) {
	if s != nil && s.Ref != "" {
		s = reg.SchemaFromRef(s.Ref)
	}
	if s == nil {
		return
	}
	switch v := v.(type) {
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(v)) {
			ps := s.Properties[k]
			if ps == nil {
				continue
			}
			if v[k] != nil {
				nulls(reg, ps, v[k], path+"."+k, errs)
				continue
			}
			if ps.Ref != "" {
				ps = reg.SchemaFromRef(ps.Ref)
			}
			if !s.Nullable && !slices.Contains(s.Required, k) && ps != nil && !ps.Nullable {
				*errs = append(*errs, &huma.ErrorDetail{Location: path + "." + k, Message: "value must not be null"})
			}
		}
	case []any:
		for i, item := range v {
			nulls(reg, s.Items, item, path+"["+strconv.Itoa(i)+"]", errs)
		}
	}
}

// newAPI builds the contract on mux.
func (s *Server) newAPI(mux *http.ServeMux) huma.API {
	api := humago.New(mux, config())
	s.register(api)
	plainIntegers(api.OpenAPI())
	openResponses(api.OpenAPI())
	return api
}

// SpecYAML is the OpenAPI 3.0 document of the contract, as committed in
// api/openapi.yaml. It needs no running server.
func SpecYAML() ([]byte, error) {
	return (&Server{}).newAPI(http.NewServeMux()).OpenAPI().DowngradeYAML()
}
