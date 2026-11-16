package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/coder/websocket"
)

// asyncAPI is the AsyncAPI document as a client reads it: its JSON form
// decoded without the Go types (the YAML file is a conversion of it).
func asyncAPI(t *testing.T) map[string]any {
	t.Helper()
	doc, err := asyncAPIDocument()
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// at walks a decoded document by keys; it fails the test where a key is missing.
func at(t *testing.T, v any, keys ...string) any {
	t.Helper()
	for _, k := range keys {
		m, ok := v.(map[string]any)
		if !ok || m[k] == nil {
			t.Fatalf("no %q in the document (path %v)", k, keys)
		}
		v = m[k]
	}
	return v
}

func obj(t *testing.T, v any, keys ...string) map[string]any {
	t.Helper()
	m, ok := at(t, v, keys...).(map[string]any)
	if !ok {
		t.Fatalf("%v is not an object", keys)
	}
	return m
}

func list(t *testing.T, v any, keys ...string) []any {
	t.Helper()
	l, ok := at(t, v, keys...).([]any)
	if !ok {
		t.Fatalf("%v is not a list", keys)
	}
	return l
}

// schemaOf resolves a {$ref: "#/components/schemas/X"} in doc.
func schemaOf(t *testing.T, doc map[string]any, ref any) map[string]any {
	t.Helper()
	r, _ := ref.(map[string]any)["$ref"].(string)
	name, ok := strings.CutPrefix(r, "#/components/schemas/")
	if !ok {
		t.Fatalf("%v is not a reference to a schema", ref)
	}
	return obj(t, doc, "components", "schemas", name)
}

// jsonSchemaOf is the JSON Schema a Go field type must have on the wire: the
// type and format encoding/json writes for it, or a reference to the named
// schema of a contract type.
func jsonSchemaOf(t reflect.Type) (typ, format, ref string) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch {
	case t == reflect.TypeFor[uuid.UUID]():
		return "string", "uuid", ""
	case t == reflect.TypeFor[time.Time]():
		return "string", "date-time", ""
	case t == reflect.TypeFor[Status]():
		return "", "", "#/components/schemas/Status"
	}
	switch t.Kind() {
	case reflect.String:
		return "string", "", ""
	case reflect.Bool:
		return "boolean", "", ""
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Uint, reflect.Uint64, reflect.Uint32:
		return "integer", "", ""
	}
	return "unmapped " + t.String(), "", ""
}

func TestEveryStreamMessagePayloadIsTheStatusEventGoType(t *testing.T) {
	doc := asyncAPI(t)
	messages := obj(t, doc, "components", "messages")
	if len(messages) == 0 {
		t.Fatal("the document has no messages")
	}
	goType := reflect.TypeFor[StatusEvent]()
	for name := range messages {
		t.Run(name, func(t *testing.T) {
			s := schemaOf(t, doc, at(t, messages, name, "payload"))
			props := obj(t, s, "properties")
			var required []string
			for _, r := range list(t, s, "required") {
				required = append(required, r.(string))
			}
			seen := map[string]bool{}
			for i := range goType.NumField() {
				f := goType.Field(i)
				tag, opts, _ := strings.Cut(f.Tag.Get("json"), ",")
				if !f.IsExported() || tag == "-" {
					continue
				}
				seen[tag] = true
				p, ok := props[tag].(map[string]any)
				if !ok {
					t.Errorf("field %s (%s) is not in the payload schema", f.Name, tag)
					continue
				}
				if want := !strings.Contains(opts, "omitempty"); slices.Contains(required, tag) != want {
					t.Errorf("%s: required is %v, the Go field's omitempty says %v", tag, !want, want)
				}
				typ, format, ref := jsonSchemaOf(f.Type)
				got := [3]any{p["type"], p["format"], p["$ref"]}
				want := [3]any{typ, format, ref}
				for i := range want {
					if want[i] == "" {
						want[i] = nil
					}
				}
				if got != want {
					t.Errorf("%s: schema (type, format, $ref) is %v, the Go type %v needs %v", tag, got, f.Type, want)
				}
				desc, _ := p["description"].(string)
				if strings.TrimSpace(desc) == "" {
					t.Errorf("%s has no description", tag)
				}
			}
			for name := range props {
				if !seen[name] {
					t.Errorf("the payload schema has %s, which StatusEvent does not", name)
				}
			}
		})
	}
}

// One schema source: the stream's payload schema is the OpenAPI document's,
// byte for byte, so the two contracts cannot drift apart.
func TestTheStreamPayloadSchemaIsTheOpenAPIDocumentsSchema(t *testing.T) {
	spec, err := (&Server{}).newAPI(http.NewServeMux()).OpenAPI().Downgrade()
	if err != nil {
		t.Fatal(err)
	}
	var oapi map[string]any
	if err := json.Unmarshal(spec, &oapi); err != nil {
		t.Fatal(err)
	}
	doc := asyncAPI(t)
	for name := range obj(t, doc, "components", "schemas") {
		t.Run(name, func(t *testing.T) {
			if got, want := at(t, doc, "components", "schemas", name), at(t, oapi, "components", "schemas", name); !reflect.DeepEqual(got, want) {
				t.Errorf("AsyncAPI\n%v\nOpenAPI\n%v", got, want)
			}
		})
	}
}

func TestEveryMessageExampleIsAValidPayload(t *testing.T) {
	doc := asyncAPI(t)
	for name, m := range obj(t, doc, "components", "messages") {
		s := schemaOf(t, doc, at(t, m, "payload"))
		for i, e := range list(t, m, "examples") {
			payload := obj(t, e, "payload")
			for _, r := range list(t, s, "required") {
				if _, ok := payload[r.(string)]; !ok {
					t.Errorf("%s example %d lacks required %s", name, i, r)
				}
			}
			for k := range payload {
				if _, ok := obj(t, s, "properties")[k]; !ok {
					t.Errorf("%s example %d has %s, which the schema does not", name, i, k)
				}
			}
		}
	}
}

func TestTheDocumentedCloseCodesAreTheOnesTheServerSends(t *testing.T) {
	type closeFrame struct {
		code   int
		reason string
	}
	var documented []closeFrame
	for _, c := range list(t, asyncAPI(t), "channels", "statusEvents", "x-close-codes") {
		reason, _ := c.(map[string]any)["reason"].(string)
		documented = append(documented, closeFrame{int(at(t, c, "code").(float64)), reason})
		ref, _ := c.(map[string]any)["reference"].(string)
		if !strings.HasPrefix(ref, "RFC 6455 section 7.4.1") && !strings.HasPrefix(ref, "IANA WebSocket Close Code Number Registry") {
			t.Errorf("close %v cites %q, not RFC 6455 section 7.4.1 or the IANA registry", c, ref)
		}
	}
	// The closes ws.go sends (pump) and the ones the WebSocket library sends
	// for it, with the codes RFC 6455 and the IANA registry give them.
	sent := []struct {
		c    streamClose
		code websocket.StatusCode
	}{
		{closeTooSlow, 1008},
		{closeFeedLost, 1012},
		{closeShutdown, 1001},
		{closeUnexpectedData, 1008},
		{closeProtocolError, 1002},
	}
	var want []closeFrame
	for _, s := range sent {
		if s.c.code != s.code {
			t.Errorf("%q is sent with %d, the standard's code is %d", s.c.reason, s.c.code, s.code)
		}
		want = append(want, closeFrame{int(s.c.code), s.c.reason})
	}
	if !slices.Equal(documented, want) {
		t.Errorf("documented closes %v, the server sends %v", documented, want)
	}
}

// pump hands the client's frames to Conn.CloseRead; the document promises
// what the library then sends for a data message. This pins the promise to
// the library version in go.mod.
func TestADataMessageFromTheClientIsClosedAsDocumented(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		<-conn.CloseRead(r.Context()).Done()
	}))
	defer srv.Close()
	conn, resp, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if resp.Body != nil { // a successful dial leaves the body to the connection
			_ = resp.Body.Close()
		}
	}()
	defer func() { _ = conn.CloseNow() }()
	if err := conn.Write(t.Context(), websocket.MessageText, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	_, _, err = conn.Read(t.Context())
	var ce websocket.CloseError
	if !errors.As(err, &ce) || ce.Code != closeUnexpectedData.code || ce.Reason != closeUnexpectedData.reason {
		t.Fatalf("got %v, documented %d %q", err, closeUnexpectedData.code, closeUnexpectedData.reason)
	}
}

func TestEveryDocumentedQueryParameterIsOneTheStreamReads(t *testing.T) {
	doc := asyncAPI(t)
	query := obj(t, doc, "channels", "statusEvents", "bindings", "ws", "query", "properties")
	var documented []string
	for name := range query {
		documented = append(documented, name)
		t.Run(name, func(t *testing.T) {
			rec := serve(t, nil, http.MethodGet, "/v1/ws?"+name+"=nope", "")
			if e := decodeError(t, rec); rec.Code != http.StatusBadRequest || len(e.Details) == 0 || e.Details[0].Field != name {
				t.Fatalf("got %d %s", rec.Code, rec.Body)
			}
			if d, _ := at(t, query, name, "description").(string); strings.TrimSpace(d) == "" {
				t.Error("no description")
			}
		})
	}
	var inSpec []string
	for _, p := range (&Server{}).newAPI(http.NewServeMux()).OpenAPI().Paths["/v1/ws"].Get.Parameters {
		inSpec = append(inSpec, p.Name)
	}
	slices.Sort(documented)
	slices.Sort(inSpec)
	if !slices.Equal(documented, inSpec) {
		t.Errorf("AsyncAPI query %v, OpenAPI parameters %v", documented, inSpec)
	}
}

func TestTheDocumented503IsWhatTheStreamAnswersWithoutLiveUpdates(t *testing.T) {
	var refusal map[string]any
	for _, r := range list(t, asyncAPI(t), "channels", "statusEvents", "x-handshake-refusals") {
		if at(t, r, "status").(float64) == http.StatusServiceUnavailable {
			refusal = r.(map[string]any)
		}
	}
	if refusal == nil {
		t.Fatal("no 503 refusal documented")
	}
	rec := serve(t, nil, http.MethodGet, "/v1/ws", "")
	if e := decodeError(t, rec); rec.Code != http.StatusServiceUnavailable || e.Code != refusal["code"] ||
		rec.Header().Get("Content-Type") != refusal["contentType"] {
		t.Fatalf("got %d %s %s, documented %v", rec.Code, rec.Header().Get("Content-Type"), rec.Body, refusal)
	}
}

func TestTheAsyncAPIDocumentIsServedAsYAML(t *testing.T) {
	rec := httptest.NewRecorder()
	asyncAPIHandler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/asyncapi.yaml", nil))
	want, err := AsyncAPIYAML()
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/yaml" || rec.Body.String() != string(want) ||
		!strings.HasPrefix(rec.Body.String(), "asyncapi: 3.0.0\n") {
		t.Fatalf("got %d %s %.80s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}
}

// api/asyncapi.yaml is generated from this package like api/openapi.yaml; a
// hand edit or a stream change without regenerating fails here before CI.
func TestCommittedAsyncAPIDocumentIsGenerated(t *testing.T) {
	want, err := AsyncAPIYAML()
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("../../api/asyncapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, body, ok := strings.Cut(strings.ReplaceAll(string(got), "\r\n", "\n"), "\nasyncapi:"); !ok || "asyncapi:"+body != string(want) {
		t.Fatal("api/asyncapi.yaml differs from the server's code: run `go generate ./internal/httpapi` (never edit the file)")
	}
}
