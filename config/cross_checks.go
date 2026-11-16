package config

import (
	"fmt"

	"github.com/LeventAksakal/notification-system/internal/delivery/leasebudget"
	"github.com/LeventAksakal/notification-system/internal/delivery/limiter"
)

// crossChecks refuses combinations of dials that cannot work together, each
// with the rule it breaks. It runs once every dial has parsed.
func crossChecks(c Config) []error {
	var errs []error
	if c.RetryCap < c.RetryBase {
		errs = append(errs, fmt.Errorf("APP_RETRY_CAP %s is below APP_RETRY_BASE %s", c.RetryCap, c.RetryBase))
	}
	if c.NotificationRetention < c.IdempotencyRetention {
		// A keyed retry must find the notification or batch its key names.
		errs = append(errs, fmt.Errorf("APP_NOTIFICATION_RETENTION %s is below APP_IDEMPOTENCY_RETENTION %s", c.NotificationRetention, c.IdempotencyRetention))
	}
	// A claimed notification waits up to the send alignment's ceiling plus
	// the horizon for its slot (leasebudget.SlotWait; both follow the
	// lease) and up to one slot interval more for a free call place; then its
	// call may run for the provider timeout and must end the lease margin
	// before the lease does, so its result is written while the claim holds.
	// A shorter lease cuts calls before the provider timeout: each cut is an
	// unknown outcome, retried, and may be a duplicate send.
	wait := leasebudget.SlotWait(c.LeaseDuration, c.RateLimitHorizon)
	interval := limiter.SlotInterval(c.RateLimitPerSecond, c.RateLimitHeadroomPct)
	if need := c.ProviderTimeout + wait + interval + leasebudget.Margin; c.LeaseDuration < need {
		errs = append(errs, fmt.Errorf("APP_LEASE_DURATION %s is below APP_PROVIDER_TIMEOUT %s + longest slot wait %s + one slot interval %s "+
			"+ lease margin %s = %s: a claimed notification waits up to the send alignment's ceiling plus the horizon (both follow "+
			"the lease) and one interval before its call, and the call must end the margin before the lease does, so a shorter "+
			"lease cuts calls before the provider timeout (raise the lease or lower the timeout)",
			c.LeaseDuration, c.ProviderTimeout, wait, interval, leasebudget.Margin, need))
	}
	// On SIGTERM a worker stops claiming and waits up to the grace for its
	// calls in flight; a call can last the provider timeout.
	if c.ShutdownTimeout < c.ProviderTimeout {
		errs = append(errs, fmt.Errorf("APP_SHUTDOWN_TIMEOUT %s is below APP_PROVIDER_TIMEOUT %s: a provider call in flight at "+
			"SIGTERM may run for the whole timeout, and the worker aborts it when the grace ends (an unknown outcome, "+
			"sent again later)", c.ShutdownTimeout, c.ProviderTimeout))
	}
	// On SIGTERM the API stops accepting and waits up to the grace for its
	// requests in flight; a request can last the request timeout.
	if c.ShutdownTimeout < c.RequestTimeout {
		errs = append(errs, fmt.Errorf("APP_SHUTDOWN_TIMEOUT %s is below APP_REQUEST_TIMEOUT %s: a request in flight at SIGTERM "+
			"may run for the whole request timeout, and the server closes it unanswered when the grace ends",
			c.ShutdownTimeout, c.RequestTimeout))
	}
	return errs
}
