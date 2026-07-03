package metrics

import "sort"

// reservoir is a bounded ring of recent samples used to estimate quantiles for
// the p99 alert comparison (R10.5) without unbounded memory growth.
type reservoir struct {
	buf  []float64
	size int
	next int
	full bool
}

func newReservoir(size int) *reservoir {
	if size <= 0 {
		size = 1
	}
	return &reservoir{buf: make([]float64, 0, size), size: size}
}

func (r *reservoir) add(v float64) {
	if !r.full {
		r.buf = append(r.buf, v)
		if len(r.buf) == r.size {
			r.full = true
			r.next = 0
		}
		return
	}
	r.buf[r.next] = v
	r.next = (r.next + 1) % r.size
}

// quantile returns the requested quantile (q in [0,1]) of the current samples
// using nearest-rank, or 0 when there are no samples.
func (r *reservoir) quantile(q float64) float64 {
	n := len(r.buf)
	if n == 0 {
		return 0
	}
	cp := make([]float64, n)
	copy(cp, r.buf)
	sort.Float64s(cp)
	if q <= 0 {
		return cp[0]
	}
	if q >= 1 {
		return cp[n-1]
	}
	idx := int(q*float64(n-1) + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return cp[idx]
}
