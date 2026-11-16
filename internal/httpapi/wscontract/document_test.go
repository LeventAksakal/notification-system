package wscontract

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
)

type inner struct {
	N int `json:"n"`
}

type outer struct {
	Items []inner `json:"items"`
}

func registry() huma.Registry {
	return huma.NewMapRegistry(schemaPrefix, huma.DefaultSchemaNamer)
}

func stream(reg huma.Registry, payload reflect.Type) Stream {
	return Stream{
		Title: "t", Version: "1.0.0",
		Server:    Server{Name: "local", Host: "localhost", Protocol: "ws"},
		Channel:   Channel{Name: "events", Address: "/ws"},
		Operation: Operation{Name: "sendEvents"},
		Messages:  []Message{{Name: "event", Payload: payload}},
		Registry:  reg,
	}
}

func TestBuild(t *testing.T) {
	t.Run("every schema a payload reaches is in components", func(t *testing.T) {
		doc, err := Build(stream(registry(), reflect.TypeFor[outer]()))
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"Outer", "Inner"} {
			if doc.Components.Schemas[name] == nil {
				t.Errorf("%s is missing from components.schemas: %v", name, doc.Components.Schemas)
			}
		}
		if got := doc.Components.Messages["event"].Payload.Ref; got != schemaPrefix+"Outer" {
			t.Errorf("payload refers to %q", got)
		}
		if got := doc.Operations["sendEvents"].Messages[0].Ref; got != "#/channels/events/messages/event" {
			t.Errorf("operation message refers to %q", got)
		}
	})
	t.Run("a payload that is not a named schema is refused", func(t *testing.T) {
		if _, err := Build(stream(registry(), reflect.TypeFor[string]())); err == nil || !strings.Contains(err.Error(), "not a named schema") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("a stream without a registry is refused", func(t *testing.T) {
		if _, err := Build(stream(nil, reflect.TypeFor[outer]())); !errors.Is(err, ErrNoRegistry) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("the YAML form is an AsyncAPI 3.0 document", func(t *testing.T) {
		doc, err := Build(stream(registry(), reflect.TypeFor[outer]()))
		if err != nil {
			t.Fatal(err)
		}
		b, err := doc.YAML()
		if err != nil || !strings.HasPrefix(string(b), "asyncapi: 3.0.0\n") {
			t.Fatalf("got %v %.60s", err, b)
		}
	})
}
