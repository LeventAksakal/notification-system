package fakeprovider

// ring is a bounded FIFO: once full, each push overwrites the oldest entry.
type ring struct {
	buf   []Request
	start int
	limit int
}

func (r *ring) push(x Request) {
	if len(r.buf) < r.limit {
		r.buf = append(r.buf, x)
		return
	}
	r.buf[r.start] = x
	r.start = (r.start + 1) % r.limit
}

func (r *ring) each(fn func(Request)) {
	for i := range len(r.buf) {
		fn(r.buf[(r.start+i)%len(r.buf)])
	}
}
