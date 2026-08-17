package worker

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/anthropp/ratelimiter/internal/config"
	"github.com/anthropp/ratelimiter/internal/coordinator"
)

// These tests run a real coordinator behind httptest and drive it through the
// real httpLeaser, verifying the wire contract (JSON field names, status
// codes, partial grants) that fakeLeaser-based tests cannot see. A field
// rename that desynced the two sides would pass every other unit test and
// fail only on the cluster.

func newIntegrationLimiter(t *testing.T, tenants map[string]float64) *Limiter {
	t.Helper()
	cfg := &config.Config{Tenants: tenants}
	cfg.Lease.Size = 10
	cfg.Lease.DurationMs = 2000
	srv := httptest.NewServer(coordinator.New(cfg).Handler())
	t.Cleanup(srv.Close)
	return NewLimiter(&httpLeaser{url: srv.URL, worker: "itest", client: srv.Client()})
}

func TestIntegrationAdmitsExactlyGlobalBudget(t *testing.T) {
	// rate 10 -> burst capacity 10; one lease drains the whole bucket, and
	// over a fast loop the refill contributes at most ~1 extra token.
	lm := newIntegrationLimiter(t, map[string]float64{"ten": 10})

	admits, rejects := 0, 0
	for i := 0; i < 30; i++ {
		switch lm.Check("ten", 1) {
		case Admit:
			admits++
		case Reject:
			rejects++
		}
	}
	if admits < 10 || admits > 11 {
		t.Fatalf("admits = %d, want 10 (burst) with at most 1 refill token", admits)
	}
	if rejects < 15 {
		t.Fatalf("rejects = %d, want the remainder rejected", rejects)
	}
}

func TestIntegrationPartialGrantWhenLeaseExceedsBudget(t *testing.T) {
	// rate 5 -> capacity 5 < lease size 10: the coordinator must issue a
	// partial grant of 5 over the wire, and the worker must admit exactly it.
	lm := newIntegrationLimiter(t, map[string]float64{"small": 5})

	admits := 0
	for i := 0; i < 20; i++ {
		if lm.Check("small", 1) == Admit {
			admits++
		}
	}
	if admits != 5 {
		t.Fatalf("admits = %d, want exactly the 5-token partial grant", admits)
	}
}

func TestIntegrationUnknownTenant404(t *testing.T) {
	lm := newIntegrationLimiter(t, map[string]float64{"ten": 10})
	if d := lm.Check("nosuch", 1); d != Unknown {
		t.Fatalf("check = %v, want Unknown via real coordinator 404", d)
	}
}

func TestIntegrationZeroGrantCarriesRetryAfter(t *testing.T) {
	lm := newIntegrationLimiter(t, map[string]float64{"ten": 10})
	for i := 0; i < 15; i++ {
		lm.Check("ten", 1) // drain the burst; ends with a zero-grant response
	}
	time.Sleep(20 * time.Millisecond) // let the zero-grant renewal land

	// The zero grant's retryAfter pacing must suppress further lease calls.
	p := lm.pool("ten")
	p.mu.Lock()
	paced := !p.nextTry.IsZero()
	p.mu.Unlock()
	if !paced {
		t.Fatal("zero-grant response did not set retry pacing; retryAfterMs likely not decoded")
	}
}
