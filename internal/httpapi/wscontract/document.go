// Package wscontract builds the AsyncAPI 3.0 document of a WebSocket
// endpoint whose messages are Go types. It writes no schema of its own: every
// payload schema is taken from the Huma registry the OpenAPI document is
// built from, so the HTTP and the WebSocket contracts share one schema
// source. The endpoint's facts (address, query parameters, messages,
// refusals, close codes) come from the producer, package httpapi.
package wscontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/yaml"
)

// asyncAPIVersion is the AsyncAPI specification the document follows
// (https://www.asyncapi.com/docs/reference/specification/v3.0.0).
const asyncAPIVersion = "3.0.0"

// wsBindingVersion is the version of the AsyncAPI WebSockets channel binding
// (github.com/asyncapi/bindings, websockets/README.md), whose fields
// `method` and `query` describe the upgrade request.
const wsBindingVersion = "0.1.0"

// schemaPrefix is where both Huma's registry and AsyncAPI 3.0 keep named
// schemas, so a reference copied from one resolves in the other unchanged.
const schemaPrefix = "#/components/schemas/"

// Stream is everything the document says about one WebSocket endpoint on
// which the server sends and the client only listens.
type Stream struct {
	Title       string
	Version     string
	Description string
	Server      Server
	Channel     Channel
	// Operation is the server sending Messages on Channel (action `send`:
	// an AsyncAPI 3.0 document describes the application that owns it).
	Operation Operation
	Messages  []Message
	// Refusals are the HTTP answers to an upgrade request that is not
	// accepted, in the order the server checks for them.
	Refusals []Refusal
	// Closes are the close frames the server sends.
	Closes []Close
	// Registry is the Huma registry the payload schemas are read from.
	Registry huma.Registry
}

// Server is where the endpoint is served.
type Server struct {
	Name, Host, Protocol, Description string
}

// Channel is the endpoint: its address and the query parameters the
// upgrade request may carry.
type Channel struct {
	Name, Address, Title, Description string
	Query                             []Param
}

// Param is one query parameter of the upgrade request.
type Param struct {
	Name   string
	Schema *huma.Schema
}

// Operation names the server's send operation.
type Operation struct {
	Name, Title, Description string
}

// Message is one kind of message the server sends; Payload is its Go type.
type Message struct {
	Name, Title, Summary, Description string
	Payload                           reflect.Type
	Examples                          []Example
}

// Example is a payload value of a message, marshalled with encoding/json as
// the server marshals the real ones.
type Example struct {
	Name, Summary string
	Payload       any
}

// Refusal is one HTTP answer to an upgrade request that is not accepted.
// Payload is the body's Go type; nil for a body that is plain text.
type Refusal struct {
	Status      int
	Code        string
	ContentType string
	When        string
	Payload     reflect.Type
}

// Close is one close frame the server sends: the code, the reason text
// (empty when it varies), when it is sent and the standard defining it.
type Close struct {
	Code      int
	Reason    string
	When      string
	Reference string
}

// Document is an AsyncAPI 3.0 document. Its JSON form is the document;
// YAML converts that form.
type Document struct {
	AsyncAPI           string                     `json:"asyncapi"`
	Info               info                       `json:"info"`
	DefaultContentType string                     `json:"defaultContentType"`
	Servers            map[string]serverObject    `json:"servers"`
	Channels           map[string]channelObject   `json:"channels"`
	Operations         map[string]operationObject `json:"operations"`
	Components         components                 `json:"components"`
}

type info struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

type serverObject struct {
	Host        string `json:"host"`
	Protocol    string `json:"protocol"`
	Description string `json:"description,omitempty"`
}

type ref struct {
	Ref string `json:"$ref"`
}

type channelObject struct {
	Address     string          `json:"address"`
	Title       string          `json:"title,omitempty"`
	Description string          `json:"description,omitempty"`
	Servers     []ref           `json:"servers"`
	Messages    map[string]ref  `json:"messages"`
	Bindings    channelBindings `json:"bindings"`
	// Refusals and Closes are specification extensions (AsyncAPI 3.0,
	// "Specification Extensions"): the specification has no object for an
	// upgrade's HTTP answers or for close frames.
	Refusals []refusalObject `json:"x-handshake-refusals,omitempty"`
	Closes   []closeObject   `json:"x-close-codes,omitempty"`
}

type channelBindings struct {
	WS wsBinding `json:"ws"`
}

type wsBinding struct {
	Method         string       `json:"method"`
	Query          *huma.Schema `json:"query,omitempty"`
	BindingVersion string       `json:"bindingVersion"`
}

type refusalObject struct {
	Status      int    `json:"status"`
	Code        string `json:"code,omitempty"`
	ContentType string `json:"contentType"`
	Description string `json:"description"`
	Payload     *ref   `json:"payload,omitempty"`
}

type closeObject struct {
	Code        int    `json:"code"`
	Reason      string `json:"reason,omitempty"`
	Description string `json:"description"`
	Reference   string `json:"reference"`
}

type operationObject struct {
	Action      string `json:"action"`
	Channel     ref    `json:"channel"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Messages    []ref  `json:"messages"`
}

type components struct {
	Messages map[string]messageObject `json:"messages"`
	Schemas  map[string]*huma.Schema  `json:"schemas"`
}

type messageObject struct {
	Name        string          `json:"name"`
	Title       string          `json:"title,omitempty"`
	Summary     string          `json:"summary,omitempty"`
	Description string          `json:"description,omitempty"`
	ContentType string          `json:"contentType"`
	Payload     ref             `json:"payload"`
	Examples    []exampleObject `json:"examples,omitempty"`
}

type exampleObject struct {
	Name    string `json:"name"`
	Summary string `json:"summary,omitempty"`
	Payload any    `json:"payload"`
}

// ErrNoRegistry is returned by Build for a Stream without a Registry.
var ErrNoRegistry = errors.New("wscontract: the stream has no schema registry")

// Build assembles the document. Every Go type it names (message payloads,
// refusal bodies) is looked up in s.Registry, and the schemas they reach are
// copied into components.schemas as they are.
func Build(s Stream) (Document, error) {
	if s.Registry == nil {
		return Document{}, ErrNoRegistry
	}
	schemas := map[string]*huma.Schema{}
	schemaRef := func(t reflect.Type) (ref, error) {
		sch := s.Registry.Schema(t, true, "")
		if sch == nil || !strings.HasPrefix(sch.Ref, schemaPrefix) {
			return ref{}, fmt.Errorf("wscontract: %v is not a named schema in the registry", t)
		}
		if err := collect(s.Registry, sch, schemas); err != nil {
			return ref{}, err
		}
		return ref{sch.Ref}, nil
	}

	channelRef := "#/channels/" + s.Channel.Name
	ch := channelObject{
		Address: s.Channel.Address, Title: s.Channel.Title, Description: s.Channel.Description,
		Servers:  []ref{{"#/servers/" + s.Server.Name}},
		Messages: map[string]ref{},
		Bindings: channelBindings{WS: wsBinding{Method: "GET", BindingVersion: wsBindingVersion}},
	}
	if len(s.Channel.Query) > 0 {
		q := &huma.Schema{Type: huma.TypeObject, Properties: map[string]*huma.Schema{}}
		for _, p := range s.Channel.Query {
			q.Properties[p.Name] = p.Schema
		}
		ch.Bindings.WS.Query = q
	}
	op := operationObject{Action: "send", Channel: ref{channelRef}, Title: s.Operation.Title, Description: s.Operation.Description}
	msgs := map[string]messageObject{}
	for _, m := range s.Messages {
		payload, err := schemaRef(m.Payload)
		if err != nil {
			return Document{}, fmt.Errorf("message %s: %w", m.Name, err)
		}
		mo := messageObject{Name: m.Name, Title: m.Title, Summary: m.Summary, Description: m.Description,
			ContentType: "application/json", Payload: payload}
		for _, e := range m.Examples {
			mo.Examples = append(mo.Examples, exampleObject(e))
		}
		msgs[m.Name] = mo
		ch.Messages[m.Name] = ref{"#/components/messages/" + m.Name}
		op.Messages = append(op.Messages, ref{channelRef + "/messages/" + m.Name})
	}
	for _, r := range s.Refusals {
		ro := refusalObject{Status: r.Status, Code: r.Code, ContentType: r.ContentType, Description: r.When}
		if r.Payload != nil {
			p, err := schemaRef(r.Payload)
			if err != nil {
				return Document{}, fmt.Errorf("refusal %d: %w", r.Status, err)
			}
			ro.Payload = &p
		}
		ch.Refusals = append(ch.Refusals, ro)
	}
	for _, c := range s.Closes {
		ch.Closes = append(ch.Closes, closeObject{Code: c.Code, Reason: c.Reason, Description: c.When, Reference: c.Reference})
	}

	return Document{
		AsyncAPI:           asyncAPIVersion,
		Info:               info{Title: s.Title, Version: s.Version, Description: s.Description},
		DefaultContentType: "application/json",
		Servers: map[string]serverObject{s.Server.Name: {
			Host: s.Server.Host, Protocol: s.Server.Protocol, Description: s.Server.Description,
		}},
		Channels:   map[string]channelObject{s.Channel.Name: ch},
		Operations: map[string]operationObject{s.Operation.Name: op},
		Components: components{Messages: msgs, Schemas: schemas},
	}, nil
}

// collect copies into out every named schema s reaches.
func collect(reg huma.Registry, s *huma.Schema, out map[string]*huma.Schema) error {
	if s == nil {
		return nil
	}
	if s.Ref != "" {
		name, ok := strings.CutPrefix(s.Ref, schemaPrefix)
		if !ok {
			return fmt.Errorf("wscontract: reference %q is outside %s", s.Ref, schemaPrefix)
		}
		if _, seen := out[name]; seen {
			return nil
		}
		target := reg.SchemaFromRef(s.Ref)
		if target == nil {
			return fmt.Errorf("wscontract: reference %q names no schema", s.Ref)
		}
		out[name] = target
		s = target
	}
	next := []*huma.Schema{s.Items, s.Not}
	next = append(next, s.OneOf...)
	next = append(next, s.AnyOf...)
	next = append(next, s.AllOf...)
	for _, p := range s.Properties {
		next = append(next, p)
	}
	if ap, ok := s.AdditionalProperties.(*huma.Schema); ok {
		next = append(next, ap)
	}
	for _, n := range next {
		if err := collect(reg, n, out); err != nil {
			return err
		}
	}
	return nil
}

// YAML is the document as YAML, converted from its JSON form (the converter
// Huma uses for the OpenAPI document, so both files read alike).
func (d Document) YAML() ([]byte, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("encode the AsyncAPI document: %w", err)
	}
	var out bytes.Buffer
	if err := yaml.Convert(&out, bytes.NewReader(b)); err != nil {
		return nil, fmt.Errorf("convert the AsyncAPI document to YAML: %w", err)
	}
	return out.Bytes(), nil
}
