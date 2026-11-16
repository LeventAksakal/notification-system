package delivery

import (
	"context"
	"sync"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// gridLimiter is an in-memory GCRA with burst 1 per channel, like the Valkey
// limiter: slots exactly interval apart, at most horizon past the lead (the
// smaller of the request's and its own). Its clock is
// a "remote" one, hours away from the local wall clock and running on the
// local monotonic clock: Grant.At is on it, as Valkey's TIME is.
type gridLimiter struct {
	mu                  sync.Mutex
	err                 error
	takes               int
	reserves            int // Takes that asked for slots (n > 0)
	givenBack, gaveBack int // GiveBack calls; those that moved the next free slot back
	interval            time.Duration
	horizon             time.Duration
	next                map[notification.Channel]time.Time
	returned            map[notification.Channel]map[time.Time]time.Time // given back below a later grant: end -> start
	start               time.Time                                        // local, monotonic
}

// remoteBase is the gridLimiter's clock at its start.
var remoteBase = time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

func newGridLimiter(interval, horizon time.Duration) *gridLimiter {
	return &gridLimiter{interval: interval, horizon: horizon, next: map[notification.Channel]time.Time{},
		returned: map[notification.Channel]map[time.Time]time.Time{}, start: time.Now()}
}

// remote is local moment t on the limiter's clock.
func (l *gridLimiter) remote(t time.Time) time.Time { return remoteBase.Add(t.Sub(l.start)) }

// local is limiter moment at on the local monotonic clock.
func (l *gridLimiter) local(at time.Time) time.Time { return l.start.Add(at.Sub(remoteBase)) }

func (l *gridLimiter) Take(_ context.Context, ch notification.Channel, r notification.SlotRequest) (notification.Grant, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.takes++
	if l.err != nil {
		return notification.Grant{}, l.err
	}
	if r.N > 0 {
		l.reserves++
	}
	now := time.Now()
	k, first, wait, tat, idle := gcraTake(l.next[ch], now, l.interval, r, l.horizon)
	if k > 0 { // as the Valkey script: a Take that reserves nothing writes nothing
		l.next[ch] = tat
	}
	return notification.Grant{N: k, At: l.remote(now), First: first, Interval: l.interval, Wait: wait, Idle: idle}, nil
}

// gcraTake is the Valkey limiter's script on time.Time: from the next free
// slot tat (zero: no key) it reserves up to r.N slots, starting a free grid
// (tat not after now) at r.Lead, and none later than r.Lead plus the smaller
// of r.Horizon and horizon (the fake's own cap; 0 for none) after now. It
// returns the slots reserved, the first one's distance from now, the wait
// before asking again can reserve something, the new next free slot and how
// long the grid had been free (Grant.Idle).
func gcraTake(tat, now time.Time, interval time.Duration, r notification.SlotRequest, horizon time.Duration) (int, time.Duration, time.Duration, time.Time, time.Duration) {
	idle := time.Duration(0) // as the Valkey limiter reports it
	switch {
	case tat.IsZero():
		idle = time.Second // the least a missing key proves (limiter.keyLinger)
	case tat.Before(now):
		idle = now.Sub(tat)
	}
	if !tat.After(now) {
		tat = now.Add(r.Lead)
	}
	h := r.Horizon
	if horizon > 0 {
		h = min(h, horizon)
	}
	last := r.Lead + h
	first := tat.Sub(now)
	k := 0
	if first <= last {
		k = min(int((last-first)/interval)+1, r.N)
	}
	tat = tat.Add(time.Duration(k) * interval)
	var wait time.Duration
	if k < r.N {
		wait = max(tat.Sub(now)-last, 0)
	}
	return k, first, wait, tat, idle
}

// GiveBack moves the channel's next free slot back to from if it is still
// g's end, and on through ranges given back earlier below it; with a later
// grant after g it remembers the range instead: as the Valkey limiter's
// script does.
func (l *gridLimiter) GiveBack(_ context.Context, ch notification.Channel, g notification.Grant, from time.Time) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.givenBack++
	if l.err != nil {
		return false, l.err
	}
	end := l.local(g.At.Add(g.First + time.Duration(g.N)*g.Interval))
	if g.N <= 0 || !l.local(from).Before(end) {
		return false, nil
	}
	if !l.next[ch].Equal(end) {
		if l.next[ch].After(end) {
			if l.returned[ch] == nil {
				l.returned[ch] = map[time.Time]time.Time{}
			}
			l.returned[ch][end] = l.local(from)
		}
		return false, nil
	}
	tat := l.local(from)
	for {
		back, ok := l.returned[ch][tat]
		if !ok {
			break
		}
		delete(l.returned[ch], tat)
		tat = back
	}
	l.next[ch] = tat
	l.gaveBack++
	return true, nil
}

func testLimiter() *gridLimiter { return newGridLimiter(5*time.Millisecond, 20*time.Millisecond) }
