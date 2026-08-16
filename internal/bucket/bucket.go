// Package bucket implements a token bucket: capacity C, refilled continuously
// at a fixed rate. All methods take an explicit `now` so callers control time.
package bucket

import (
	"math"
	"sync"
	"time"
)

type Bucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	cap    float64
	tokens float64
	last   time.Time
}

// New returns a bucket that starts full. Starting full means a freshly
// (re)started coordinator can briefly over-admit up to one burst; the spec
// accepts this in exchange for keeping all state in memory.
func New(rate, capacity float64, now time.Time) *Bucket {
	return &Bucket{rate: rate, cap: capacity, tokens: capacity, last: now}
}

func (b *Bucket) refillLocked(now time.Time) {
	if now.After(b.last) {
		b.tokens = math.Min(b.cap, b.tokens+now.Sub(b.last).Seconds()*b.rate)
		b.last = now
	}
}

// TakeUpTo removes and returns up to n whole tokens; 0 if less than one is
// available. Partial grants let a worker keep serving a tenant whose global
// budget is nearly exhausted.
func (b *Bucket) TakeUpTo(n int, now time.Time) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	take := int(b.tokens)
	if take <= 0 {
		return 0
	}
	if take > n {
		take = n
	}
	b.tokens -= float64(take)
	return take
}

// NextTokenIn reports how long until at least one whole token is available.
func (b *Bucket) NextTokenIn(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	if b.tokens >= 1 {
		return 0
	}
	return time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
}
