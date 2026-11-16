package provider

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func TestClassify(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	hdr := func(v string) http.Header { return http.Header{"Retry-After": {v}} }
	cases := []struct {
		name       string
		status     int
		err        error
		h          http.Header
		want       notification.AttemptOutcome
		retryAfter time.Duration
		fault      bool
	}{
		{"202 is accepted", 202, nil, nil, notification.OutcomeAccepted, 0, false},
		{"200 is accepted", 200, nil, nil, notification.OutcomeAccepted, 0, false},
		{"network error is transient", 0, errors.New("connection refused"), nil, notification.OutcomeTransient, 0, true},
		{"timeout is transient", 0, ErrTimeout, nil, notification.OutcomeTransient, 0, true},
		{"408 is transient", 408, nil, nil, notification.OutcomeTransient, 0, true},
		{"425 is transient", 425, nil, nil, notification.OutcomeTransient, 0, true},
		{"429 with seconds", 429, nil, hdr("2"), notification.OutcomeTransient, 2 * time.Second, true},
		{"503 with an HTTP date", 503, nil, hdr(now.Add(90 * time.Second).Format(http.TimeFormat)), notification.OutcomeTransient, 90 * time.Second, true},
		{"503 with a past date waits nothing extra", 503, nil, hdr(now.Add(-time.Minute).Format(http.TimeFormat)), notification.OutcomeTransient, 0, true},
		{"503 with garbage Retry-After", 503, nil, hdr("soon"), notification.OutcomeTransient, 0, true},
		{"500 is transient", 500, nil, nil, notification.OutcomeTransient, 0, true},
		{"599 is transient", 599, nil, nil, notification.OutcomeTransient, 0, true},
		{"huge Retry-After is capped", 429, nil, hdr("99999999999"), notification.OutcomeTransient, maxRetryAfter, true},
		{"400 is permanent", 400, nil, nil, notification.OutcomePermanent, 0, false},
		{"401 is permanent", 401, nil, nil, notification.OutcomePermanent, 0, false},
		{"404 is permanent", 404, nil, nil, notification.OutcomePermanent, 0, false},
		{"422 is permanent", 422, nil, nil, notification.OutcomePermanent, 0, false},
		{"Retry-After on a 400 is ignored", 400, nil, hdr("5"), notification.OutcomePermanent, 0, false},
		{"a redirect is permanent", 302, nil, nil, notification.OutcomePermanent, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := classifyAt(now, c.status, c.err, c.h)
			if v.Outcome != c.want || v.RetryAfter != c.retryAfter || v.ProviderFault != c.fault {
				t.Errorf("got %+v, want outcome %s retryAfter %s fault %v", v, c.want, c.retryAfter, c.fault)
			}
			if (v.Outcome == notification.OutcomeAccepted) != (v.Error == "") {
				t.Errorf("error text %q for outcome %s", v.Error, v.Outcome)
			}
		})
	}
}
