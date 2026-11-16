package httpapi

import (
	"net/http"
	"testing"
)

func TestStreamAndMetricsRefuseBrokenParametersByName(t *testing.T) {
	for _, tc := range []struct{ path, field string }{
		{"/v1/ws?notificationId=nope", "notificationId"},
		{"/v1/ws?batchId=123", "batchId"},
		{"/v1/metrics?windowSeconds=0", "windowSeconds"},
		{"/v1/metrics?windowSeconds=3601", "windowSeconds"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			rec := serve(t, nil, http.MethodGet, tc.path, "")
			e := decodeError(t, rec)
			if rec.Code != http.StatusBadRequest || e.Code != "invalid_request" || e.Details == nil || e.Details[0].Field != tc.field {
				t.Fatalf("got %d %s", rec.Code, rec.Body)
			}
		})
	}
}

func TestMalformedParametersAre400WithTheErrorBody(t *testing.T) {
	for _, tc := range []struct{ method, path, field string }{
		{http.MethodGet, "/v1/notifications/not-a-uuid", "id"},
		{http.MethodPost, "/v1/notifications/not-a-uuid/cancel", "id"},
		{http.MethodGet, "/v1/batches/not-a-uuid", "id"},
		{http.MethodGet, "/v1/notifications?status=sent&status=failed", "status"},
		{http.MethodGet, "/v1/notifications?channel=sms&channel=push", "channel"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := serve(t, nil, tc.method, tc.path, "")
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type %q, want application/json", ct)
			}
			e := decodeError(t, rec)
			if rec.Code != http.StatusBadRequest || e.Code != "invalid_request" || e.Details == nil || e.Details[0].Field != tc.field {
				t.Fatalf("got %d %s", rec.Code, rec.Body)
			}
		})
	}
}

// Huma's uuid format accepts a "urn:uuid:" prefix in any case; the standard
// library parses it only in lower case, so UUID parameters are read by
// parseUUID.
func TestEveryUUIDHumaAcceptsIsParsed(t *testing.T) {
	for _, s := range []string{
		"3f2504e0-4f89-11d3-9a0c-0305e82c3301",
		"3F2504E0-4F89-11D3-9A0C-0305E82C3301",
		"urn:uuid:3f2504e0-4f89-11d3-9a0c-0305e82c3301",
		"URN:UUID:3f2504e0-4f89-11d3-9a0c-0305e82c3301",
	} {
		var p uuidParam
		if err := p.UnmarshalText([]byte(s)); err != nil || p.String() != "3f2504e0-4f89-11d3-9a0c-0305e82c3301" {
			t.Errorf("%s: %v %v", s, p, err)
		}
	}
}
