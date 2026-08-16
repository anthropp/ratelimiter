// Package worker implements the local admission decision: per-tenant pools of
// leased tokens, refilled from the coordinator on demand and by low-watermark
// prefetch. The admission path never waits on the coordinator (design D2),
// with one bounded exception: a cold or expired pool may wait up to 25ms for
// the in-flight lease rather than return a spurious 429.
package worker

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Grant is one successful-or-zero lease response.
type Grant struct {
	Tokens     int
	TTL        time.Duration
	RetryAfter time.Duration // meaningful when Tokens == 0
}

// ErrUnknownTenant marks a coordinator 404, which is negative-cached.
var ErrUnknownTenant = errors.New("unknown tenant")

// Leaser fetches one lease from the coordinator. Implementations must apply
// their own request timeout.
type Leaser interface {
	Lease(ctx context.Context, tenant string) (Grant, error)
}

const (
	prefetchFraction = 0.2 // renew when pool falls below this fraction of the last grant
	minRetryAfter    = 200 * time.Millisecond
	backoffMin       = 250 * time.Millisecond
	backoffMax       = 2 * time.Second
	unknownTenantTTL = 10 * time.Second
	// waitForLease bounds how long an empty-pool request may wait for an
	// in-flight renewal. Only requests with no known-exhausted signal wait
	// (cold or expired pools); tenants in zero-grant pacing or coordinator
	// backoff fail fast. This turns cold-start 429s for under-limit tenants
	// into a one-RTT (~2ms) delay.
	waitForLease = 25 * time.Millisecond
)

type pool struct {
	mu           sync.Mutex
	tokens       int
	expiry       time.Time // tokens beyond this instant are stale and dropped
	lastGrant    int
	renewing     bool          // singleflight: at most one lease call in flight per tenant
	renewDone    chan struct{} // closed when the in-flight renewal completes
	nextTry      time.Time
	backoff      time.Duration
	unknownUntil time.Time
}

type Decision int

const (
	Admit Decision = iota
	Reject
	Unknown
)

type Counters struct {
	Admitted   atomic.Int64
	Rejected   atomic.Int64
	LeaseCalls atomic.Int64
	LeaseErrs  atomic.Int64
}

type Limiter struct {
	mu     sync.Mutex
	pools  map[string]*pool
	leaser Leaser
	now    func() time.Time
	// Counts is keyed by tenant; populated lazily alongside pools.
	counts   map[string]*Counters
	countsMu sync.RWMutex
}

func NewLimiter(l Leaser) *Limiter {
	return &Limiter{pools: make(map[string]*pool), counts: make(map[string]*Counters), leaser: l, now: time.Now}
}

func (lm *Limiter) pool(tenant string) *pool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	p, ok := lm.pools[tenant]
	if !ok {
		p = &pool{}
		lm.pools[tenant] = p
	}
	return p
}

func (lm *Limiter) counters(tenant string) *Counters {
	lm.countsMu.RLock()
	c, ok := lm.counts[tenant]
	lm.countsMu.RUnlock()
	if ok {
		return c
	}
	lm.countsMu.Lock()
	defer lm.countsMu.Unlock()
	if c, ok = lm.counts[tenant]; !ok {
		c = &Counters{}
		lm.counts[tenant] = c
	}
	return c
}

// Check makes the admission decision for one request.
func (lm *Limiter) Check(tenant string) Decision {
	p := lm.pool(tenant)
	now := lm.now()

	p.mu.Lock()
	if now.Before(p.unknownUntil) {
		p.mu.Unlock()
		return Unknown
	}
	if p.tokens > 0 && !now.Before(p.expiry) {
		p.tokens = 0 // grant expired; drop stale budget (fail-closed direction)
	}
	admitted := p.tokens > 0
	if admitted {
		p.tokens--
	}
	needRenew := lm.shouldRenewLocked(p, now)
	if needRenew {
		p.renewing = true
		p.renewDone = make(chan struct{})
	}
	// An empty pool with a renewal in flight is a cold start only when there
	// is no known-exhausted signal (no zero-grant pacing, no backoff) AND no
	// recent grant (never leased, or the last grant expired while idle —
	// e.g. a connection re-pinned to a worker idle for this tenant). Briefly
	// wait for the renewal then, instead of returning a spurious 429. A pool
	// that merely drained a recent grant rejects immediately: waiting on
	// every racing exhaustion would serialize high-rate tenants behind
	// singleflight renewals.
	cold := p.lastGrant == 0 || !now.Before(p.expiry)
	canWait := !admitted && p.renewing && p.nextTry.IsZero() && cold
	done := p.renewDone
	p.mu.Unlock()

	if needRenew {
		go lm.renew(tenant, p)
	}
	if canWait && done != nil {
		select {
		case <-done:
			now = lm.now()
			p.mu.Lock()
			if now.Before(p.unknownUntil) {
				p.mu.Unlock()
				return Unknown
			}
			if p.tokens > 0 && now.Before(p.expiry) {
				p.tokens--
				admitted = true
			}
			p.mu.Unlock()
		case <-time.After(waitForLease):
		}
	}
	c := lm.counters(tenant)
	if admitted {
		c.Admitted.Add(1)
		return Admit
	}
	c.Rejected.Add(1)
	return Reject
}

func (lm *Limiter) shouldRenewLocked(p *pool, now time.Time) bool {
	if p.renewing || now.Before(p.nextTry) {
		return false
	}
	watermark := int(float64(p.lastGrant) * prefetchFraction)
	if watermark < 1 {
		watermark = 1
	}
	return p.tokens < watermark || p.lastGrant == 0
}

func (lm *Limiter) renew(tenant string, p *pool) {
	c := lm.counters(tenant)
	c.LeaseCalls.Add(1)
	g, err := lm.leaser.Lease(context.Background(), tenant)
	now := lm.now()

	p.mu.Lock()
	defer p.mu.Unlock()
	p.renewing = false
	if p.renewDone != nil {
		close(p.renewDone)
		p.renewDone = nil
	}
	switch {
	case errors.Is(err, ErrUnknownTenant):
		p.tokens = 0
		p.unknownUntil = now.Add(unknownTenantTTL)
	case err != nil:
		c.LeaseErrs.Add(1)
		// Coordinator unreachable: exponential backoff with +/-25% jitter.
		if p.backoff == 0 {
			p.backoff = backoffMin
		} else {
			p.backoff *= 2
			if p.backoff > backoffMax {
				p.backoff = backoffMax
			}
		}
		jitter := 1 + (rand.Float64()-0.5)/2
		p.nextTry = now.Add(time.Duration(float64(p.backoff) * jitter))
	case g.Tokens == 0:
		// Coordinator healthy but tenant's global budget is exhausted right
		// now. Honor its pacing hint; do not treat as an error.
		p.backoff = 0
		ra := g.RetryAfter
		if ra < minRetryAfter {
			ra = minRetryAfter
		}
		p.nextTry = now.Add(ra)
	default:
		p.backoff = 0
		p.nextTry = time.Time{}
		p.tokens += g.Tokens
		p.expiry = now.Add(g.TTL)
		p.lastGrant = g.Tokens
	}
}

// StatsSnapshot returns per-tenant counter values.
func (lm *Limiter) StatsSnapshot() map[string]map[string]int64 {
	lm.countsMu.RLock()
	defer lm.countsMu.RUnlock()
	out := make(map[string]map[string]int64, len(lm.counts))
	for tenant, c := range lm.counts {
		out[tenant] = map[string]int64{
			"admitted":   c.Admitted.Load(),
			"rejected":   c.Rejected.Load(),
			"leaseCalls": c.LeaseCalls.Load(),
			"leaseErrs":  c.LeaseErrs.Load(),
		}
	}
	return out
}
