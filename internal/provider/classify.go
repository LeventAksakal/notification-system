package provider

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// Verdict is what one provider call means for the notification.
type Verdict struct {
	Outcome notification.AttemptOutcome
	// RetryAfter is the provider's Retry-After, 0 when absent or unparsable.
	RetryAfter time.Duration
	// Error describes a failed call for last_error; "" when accepted.
	Error string
	// ProviderFault is true when the call says the provider is unhealthy
	// (it counts as a fault for the shared provider health, decision 0026).
	ProviderFault bool
}

// Classify is the only place that decides accepted, transient or permanent
// (decision 0007). status is 0 when no response arrived and err says why.
//
//	2xx                                   accepted (with or without messageId)
//	no response, timeout, 408, 425, 429, 5xx  transient
//	any other status                      permanent
func Classify(status int, err error, h http.Header) Verdict {
	return classifyAt(time.Now(), status, err, h)
}

func classifyAt(now time.Time, status int, err error, h http.Header) Verdict {
	if status == 0 {
		msg := "no response"
		if err != nil {
			msg = err.Error()
			if errors.Is(err, ErrTimeout) {
				msg = "timeout: " + msg
			}
		}
		return Verdict{Outcome: notification.OutcomeTransient, Error: msg, ProviderFault: true}
	}
	switch {
	case status >= 200 && status < 300:
		return Verdict{Outcome: notification.OutcomeAccepted}
	case status == http.StatusRequestTimeout, status == http.StatusTooEarly,
		status == http.StatusTooManyRequests, status >= 500 && status < 600:
		return Verdict{
			Outcome:       notification.OutcomeTransient,
			RetryAfter:    retryAfter(now, h.Get("Retry-After")),
			Error:         statusError(status),
			ProviderFault: true,
		}
	default:
		return Verdict{Outcome: notification.OutcomePermanent, Error: statusError(status)}
	}
}

func statusError(status int) string {
	if t := http.StatusText(status); t != "" {
		return fmt.Sprintf("HTTP %d %s", status, t)
	}
	return fmt.Sprintf("HTTP %d", status)
}

// maxRetryAfter bounds a provider's Retry-After so a bogus value cannot park
// a notification for ever.
const maxRetryAfter = 24 * time.Hour

// retryAfter parses Retry-After as delay-seconds or an HTTP date (RFC 9110
// §10.2.3); 0 when absent, unparsable or in the past.
func retryAfter(now time.Time, v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	var d time.Duration
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		if secs > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		d = time.Duration(secs) * time.Second
	} else if at, err := http.ParseTime(v); err == nil {
		d = at.Sub(now)
	}
	return min(max(d, 0), maxRetryAfter)
}
