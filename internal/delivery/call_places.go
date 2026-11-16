package delivery

import (
	"context"
	"time"
)

// callPlaces are the places a channel's provider calls take: the channel's
// own first, then the process's shared ones.
type callPlaces struct {
	own    chan struct{}
	shared chan struct{}
}

// full reports whether no place is free now.
func (p callPlaces) full() bool {
	return len(p.own) == cap(p.own) && len(p.shared) == cap(p.shared)
}

// take takes a place, waiting until deadline at most; it returns the func
// that gives the place back, or false when none came free in time or ctx ended.
func (p callPlaces) take(ctx context.Context, deadline time.Time) (func(), bool) {
	giveOwn := func() { <-p.own }
	giveShared := func() { <-p.shared }
	select {
	case p.own <- struct{}{}:
		return giveOwn, true
	default:
	}
	select {
	case p.shared <- struct{}{}:
		return giveShared, true
	default:
	}
	t := time.NewTimer(time.Until(deadline))
	defer t.Stop()
	select {
	case p.own <- struct{}{}:
		return giveOwn, true
	case p.shared <- struct{}{}:
		return giveShared, true
	case <-t.C:
	case <-ctx.Done():
	}
	return nil, false
}
