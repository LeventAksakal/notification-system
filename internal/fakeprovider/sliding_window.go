package fakeprovider

import (
	"time"
)

// window tracks the most events (requests or deliveries) in any 1-second span [t, t+1s).
// It keeps only timestamps from the last second, so memory follows the peak
// rate, not the total.
type window struct {
	recent []time.Time
	max    int
}

func (w *window) add(t time.Time) {
	i := 0
	for i < len(w.recent) && t.Sub(w.recent[i]) >= time.Second {
		i++
	}
	w.recent = append(w.recent[i:], t)
	w.max = max(w.max, len(w.recent))
}
