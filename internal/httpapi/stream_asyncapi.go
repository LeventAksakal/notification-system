package httpapi

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"time"
	"uuid"

	"github.com/danielgtaylor/huma/v2"

	"github.com/LeventAksakal/notification-system/internal/httpapi/wscontract"
)

// AsyncAPIYAML is the AsyncAPI 3.0 document of the WebSocket stream
// (GET /v1/ws), as committed in api/asyncapi.yaml. Like SpecYAML it needs no
// running server, and it reads its payload schemas from the same Huma
// registry, so a field added to StatusEvent appears in both documents.
func AsyncAPIYAML() ([]byte, error) {
	doc, err := asyncAPIDocument()
	if err != nil {
		return nil, err
	}
	return doc.YAML()
}

// asyncAPIDocument builds the contract's OpenAPI document and describes the
// stream with its registry.
func asyncAPIDocument() (wscontract.Document, error) {
	oapi := (&Server{}).newAPI(http.NewServeMux()).OpenAPI()
	s, err := streamContract(oapi)
	if err != nil {
		return wscontract.Document{}, err
	}
	return wscontract.Build(s)
}

// streamContract is what the AsyncAPI document says about GET /v1/ws. Every
// fact is read from the code the stream runs: the query parameters ws.go
// reads, the message type it writes, the errors it answers with, the close
// codes it sends and its ping and write timings.
func streamContract(oapi *huma.OpenAPI) (wscontract.Stream, error) {
	if len(oapi.Servers) == 0 {
		return wscontract.Stream{}, fmt.Errorf("the OpenAPI document names no server")
	}
	server, err := url.Parse(oapi.Servers[0].URL)
	if err != nil {
		return wscontract.Stream{}, fmt.Errorf("the OpenAPI server URL: %w", err)
	}

	query := make([]wscontract.Param, 0, len(streamQuery))
	for _, p := range streamQuery {
		query = append(query, wscontract.Param{Name: p.name,
			Schema: &huma.Schema{Type: huma.TypeString, Format: "uuid", Description: p.doc}})
	}

	closes := make([]wscontract.Close, 0, len(streamCloses))
	for _, c := range streamCloses {
		closes = append(closes, wscontract.Close{Code: int(c.code), Reason: c.reason, When: c.when, Reference: c.standard})
	}

	errorType := reflect.TypeFor[Error]()
	invalidQuery, missing, internal := invalid(nil), notFound("notification"), errInternal()
	refusals := []wscontract.Refusal{
		{Status: invalidQuery.status, Code: invalidQuery.Code, ContentType: "application/json", Payload: errorType,
			When: "A query parameter is not a UUID; `details[].field` names it. Fix the request; repeating it fails the same way."},
		{Status: http.StatusServiceUnavailable, Code: errLiveUpdatesUnavailable().Code, ContentType: "application/json",
			Payload: errorType, When: liveUpdatesUnavailable + "."},
		{Status: missing.status, Code: missing.Code, ContentType: "application/json", Payload: errorType,
			When: "`notificationId` or `batchId` names a notification or batch that does not exist; `message` says which. " +
				"Do not retry with the same id."},
		{Status: internal.status, Code: internal.Code, ContentType: "application/json", Payload: errorType,
			When: "Looking up the notification or batch failed unexpectedly. Retry with backoff."},
		{Status: http.StatusUpgradeRequired, ContentType: "text/plain",
			When: "The request is not a WebSocket upgrade: HTTP/1.0, or no `Connection: Upgrade` and `Upgrade: websocket` " +
				"(RFC 6455 section 4.2.1). Checked after the refusals above, so a plain GET learns about bad ids first."},
		{Status: http.StatusBadRequest, ContentType: "text/plain",
			When: "`Sec-WebSocket-Version` is not 13, or `Sec-WebSocket-Key` is missing, repeated or not 16 bytes in base64 " +
				"(RFC 6455 section 4.2.1). WebSocket client libraries send these right; this is a hand-made handshake."},
		{Status: http.StatusForbidden, ContentType: "text/plain",
			When: "The `Origin` header names a host other than the request's `Host`: a browser page served from another " +
				"origin may not open the stream (RFC 6455 section 10.2). Clients outside a browser send no `Origin`."},
	}

	example := streamExamples()
	return wscontract.Stream{
		Title:   "Notification system status stream",
		Version: Version,
		Description: "The WebSocket half of the Notification system contract. Like the OpenAPI document (api/openapi.yaml), " +
			"it is generated from the server's code (internal/httpapi), and its payload schemas are that document's, by the " +
			"same names. They are JSON Schema as Huma writes it, within what AsyncAPI's default schema format (a superset " +
			"of JSON Schema draft 07) reads. A field may be added to a message in a minor version: ignore fields you do " +
			"not know.",
		Server: wscontract.Server{Name: "local", Host: server.Host, Protocol: "ws",
			Description: "The development server, as in the OpenAPI document. Behind TLS the protocol is `wss` and the host " +
				"is the deployment's."},
		Channel: wscontract.Channel{
			Name: "statusEvents", Address: "/v1/ws", Title: "Status changes of notifications",
			Description: streamDescription(), Query: query,
		},
		Operation: wscontract.Operation{Name: "sendStatusEvents", Title: "Send status events",
			Description: "The server sends these on every accepted stream; the client only receives."},
		Messages: []wscontract.Message{
			{Name: "snapshot", Title: "Snapshot", Payload: reflect.TypeFor[StatusEvent](),
				Summary: "The notification's current status, first on a `notificationId` subscription",
				Description: "Sent once, as the first message, when the subscription names `notificationId`, and never " +
					"otherwise. `snapshot` is true. `status` is the notification's status as GET /v1/notifications/{id} " +
					"reads it, so it may be `scheduled`. `seq` is the status sequence the snapshot shows: every later " +
					"message about this notification has a greater one.",
				Examples: []wscontract.Example{{Name: "pendingSnapshot",
					Summary: "A notification waiting for its first attempt", Payload: example.snapshot}}},
			{Name: "statusChange", Title: "Status change", Payload: reflect.TypeFor[StatusEvent](),
				Summary: "One committed status change of a notification the subscription matches",
				Description: "Sent after each status change is committed. A notification's messages arrive in increasing " +
					"`seq` without repeats, and never one older than what the stream has already sent; messages of " +
					"different notifications have no order among them. A gap in `seq` means a change was lost (its " +
					"event never arrived, or arrived after the server stopped waiting for it): read the notification " +
					"over REST. `snapshot` is absent. Without `seq` (an event from a server older than the sequence) " +
					"the message is passed on unordered.",
				Examples: []wscontract.Example{
					{Name: "sent", Summary: "The provider accepted the notification", Payload: example.sent},
					{Name: "retrying", Summary: "An attempt failed and the notification waits for a retry", Payload: example.retry},
				}},
		},
		Refusals: refusals,
		Closes:   closes,
		Registry: oapi.Components.Schemas,
	}, nil
}

// streamDescription is the channel's description: what to expect on an
// accepted stream, with the timings ws.go uses.
func streamDescription() string {
	return "Upgrade GET /v1/ws to a WebSocket and receive notifications' status changes: each text message is one " +
		"`StatusEvent` as JSON. The REST API stays the source of truth: the stream says what changed, and a client " +
		"that may have missed something reads the notification. The query parameters (the `ws` binding's `query`) " +
		"choose which notifications' changes follow.\n\n" +
		"Clients send no messages. A data message from the client ends the stream with close code 1008 " +
		"(`unexpected data message`); ping and close frames are answered (RFC 6455 section 5.5).\n\n" +
		"The server pings every " + wsPingEvery.String() + "; WebSocket libraries answer with a pong on their own. " +
		"When a message or a ping cannot be written to the client within " + wsWriteTimeout.String() + ", or the " +
		"client is gone, the server drops the connection without a close frame; client libraries then report 1006, " +
		"a code that is never sent (RFC 6455 section 7.4.1). Reconnect.\n\n" +
		"The server ends a stream with a close frame from `x-close-codes` (code, reason text, when, standard). It " +
		"closes rather than go on with a hole: on any of them, reconnect and read the current state. After 1012, " +
		"expect 503 until the event feed is back, and retry with backoff.\n\n" +
		"An upgrade that is not accepted is answered over HTTP, as listed in `x-handshake-refusals` in the order " +
		"the server checks."
}

// streamExampleSet holds the example payloads, built as the stream builds
// its messages, so they are valid StatusEvents by construction.
type streamExampleSet struct {
	snapshot, sent, retry StatusEvent
}

// The values are sample data, not settings: seq 1 is creation, seq 3 the
// result of the first attempt (2 was its claim, 1 to processing), two
// seconds after creation.
func streamExamples() streamExampleSet {
	id := uuid.MustParse("0193a2c4-5e6f-7a8b-9c0d-1e2f3a4b5c6d")
	correlation := "6f1c2d3e-4a5b-4c6d-8e7f-9a0b1c2d3e4f"
	created := time.Date(2026, time.November, 16, 9, 30, 0, 0, time.UTC)
	snapshot := true
	providerError := "provider answered 503 Service Unavailable"
	return streamExampleSet{
		snapshot: StatusEvent{NotificationID: id, Status: StatusPending, At: created, CorrelationID: correlation,
			Seq: 1, Snapshot: &snapshot},
		sent: StatusEvent{NotificationID: id, Status: StatusSent, Attempts: 1, At: created.Add(2 * time.Second),
			CorrelationID: correlation, Seq: 3},
		retry: StatusEvent{NotificationID: id, Status: StatusPending, Attempts: 1, Error: &providerError,
			At: created.Add(2 * time.Second), CorrelationID: correlation, Seq: 3},
	}
}
