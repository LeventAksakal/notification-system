package delivery

import (
	"context"
	"slices"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// claim claims up to n due notifications of the channel: the selector splits
// n by weight, then each priority is asked once, high to low, for its share
// plus what the priorities above it could not use. What is still unused after
// low goes up to the priorities that may have more. It returns the priorities
// that returned fewer rows than asked (drained) and the round they belong to:
// the caller settles them with settleEmpty once the sends are started.
// queried reports whether the database was asked at all.
//
// A priority is not asked only to fill slots another could not use when it
// is known to be empty, its safety poll is due, or it was never answered for
// (selector.known: a priority with no weight is reached only that way). Each
// such ask is one more claim query in the same claim, and while the database
// is slow (owner item 0011: 1.6 s a query) one short priority tripled the
// claim time, every worker missed its whole group at once and the channel
// stopped for 4 s. Those priorities are returned in polled instead: the
// caller asks Queue.NextDue for them after the sends are started, and the
// next round claims one that has a row due.
func (w *Worker) claim(ctx context.Context, cr *channelRun, n int) (jobs []notification.Job, drained, polled []int, r round, queried bool, err error) {
	if n <= 0 {
		return nil, nil, nil, round{}, false, nil
	}
	now := time.Now()
	cr.mu.Lock()
	r = cr.sel.beginRound()
	counts := cr.sel.plan(n, now)
	var empty, unknown [3]bool
	for i := range empty {
		empty[i] = cr.sel.empty(i, now)
		unknown[i] = !cr.sel.known[i]
	}
	_, polled = cr.sel.due(now) // safety polls due: NextDue, not claimed
	cr.mu.Unlock()

	var short [3]bool // asked and got fewer than asked
	ask := func(i, k int) (int, error) {
		queried = true
		unknown[i] = false
		got, err := w.deps.Queue.Claim(ctx, cr.ch, priorityName(i), k, w.cfg.Lease)
		if err != nil {
			return 0, err
		}
		jobs = append(jobs, got...)
		if len(got) < k {
			short[i] = true
		} else {
			cr.mu.Lock()
			cr.sel.markBusy(i)
			cr.mu.Unlock()
		}
		return len(got), nil
	}
	// lookUp records that a borrowed slot skipped priority i because it was
	// never answered for: NextDue answers it after the claim.
	lookUp := func(i int) {
		if !slices.Contains(polled, i) {
			polled = append(polled, i)
		}
	}
	carry := 0
	for i := range 3 {
		k := counts[i] + carry
		carry = 0
		if k == 0 {
			continue
		}
		if empty[i] {
			carry = k
			continue
		}
		if counts[i] == 0 && unknown[i] {
			carry = k
			lookUp(i)
			continue
		}
		var got int
		if got, err = ask(i, k); err != nil {
			break
		}
		carry = k - got
	}
	for i := 2; err == nil && i >= 0 && carry > 0; i-- {
		if empty[i] || short[i] {
			continue
		}
		if unknown[i] {
			lookUp(i)
			continue
		}
		var got int
		if got, err = ask(i, carry); err != nil {
			break
		}
		carry -= got
	}
	for i, d := range short {
		if d {
			drained = append(drained, i)
		}
	}
	return jobs, drained, polled, r, queried, err
}
