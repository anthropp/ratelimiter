package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeLeaser returns scripted grants and records call counts.
type fakeLeaser struct {
	mu    sync.Mutex
	calls int
	grant Grant
	err   error
	// block, when non-nil, is closed by the test to release in-flight leases.
	block chan struct{}
}

func (f *fakeLeaser) Lease(_ context.Context, _ string) (Grant, error) {
	f.mu.Lock()
	f.calls++
	block := f.block
	g, err := f.grant, f.err
	f.mu.Unlock()
	if block != nil {
		<-block
	}
	return g, err
}

func (f *fakeLeaser) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// waitFor polls until cond is true or the deadline passes.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}

func TestFirstRequestWaitsForInitialLeaseAndAdmits(t *testing.T) {
	fl := &fakeLeaser{grant: Grant{Tokens: 10, TTL: 2 * time.Second}}
	lm := NewLimiter(fl)

	// Cold start: the first request briefly waits for the initial lease
	// instead of returning a spurious 429.
	if d := lm.Check("a", 1); d != Admit {
		t.Fatalf("first check = %v, want Admit (waits for initial lease)", d)
	}
}

func TestAdmitsExactlyGrantedTokens(t *testing.T) {
	fl := &fakeLeaser{grant: Grant{Tokens: 5, TTL: time.Minute}}
	lm := NewLimiter(fl)
	if d := lm.Check("a", 1); d != Admit { // consumes 1 of the 5
		t.Fatalf("first check = %v, want Admit", d)
	}

	// Make the next grants zero so we can count precisely.
	fl.mu.Lock()
	fl.grant = Grant{Tokens: 0, RetryAfter: time.Hour}
	fl.mu.Unlock()

	admits := 0
	for i := 0; i < 20; i++ {
		if lm.Check("a", 1) == Admit {
			admits++
		}
	}
	if admits != 4 {
		t.Fatalf("admitted %d, want exactly the 4 remaining granted tokens", admits)
	}
}

func TestSingleflightOneLeaseCallUnderConcurrency(t *testing.T) {
	fl := &fakeLeaser{grant: Grant{Tokens: 10, TTL: time.Minute}, block: make(chan struct{})}
	lm := NewLimiter(fl)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); lm.Check("a", 1) }()
	}
	wg.Wait()
	if got := fl.callCount(); got != 1 {
		t.Fatalf("lease calls = %d, want 1 (singleflight)", got)
	}
	close(fl.block)
}

func TestPrefetchRenewsBeforeExhaustion(t *testing.T) {
	fl := &fakeLeaser{grant: Grant{Tokens: 10, TTL: time.Minute}}
	lm := NewLimiter(fl)
	lm.Check("a", 1)
	waitFor(t, func() bool { return lm.Check("a", 1) == Admit })

	// Drain to just above the 20% watermark (2 of 10): no renewal expected
	// beyond the initial one.
	for lm.counters("a").Admitted.Load() < 7 {
		lm.Check("a", 1)
	}
	base := fl.callCount()

	// Crossing the watermark must trigger a background renewal even though
	// tokens remain -> no rejects at the boundary.
	for lm.counters("a").Admitted.Load() < 9 {
		if d := lm.Check("a", 1); d == Reject {
			t.Fatal("rejected while tokens remained")
		}
	}
	waitFor(t, func() bool { return fl.callCount() > base })
}

func TestExpiredTokensAreDropped(t *testing.T) {
	fl := &fakeLeaser{grant: Grant{Tokens: 10, TTL: 50 * time.Millisecond}}
	lm := NewLimiter(fl)
	var fakeNow atomic.Pointer[time.Time]
	start := time.Now()
	fakeNow.Store(&start)
	lm.now = func() time.Time { return *fakeNow.Load() }

	lm.Check("a", 1)
	waitFor(t, func() bool { return lm.Check("a", 1) == Admit })

	// Advance past the TTL; block further grants to observe the drop.
	fl.mu.Lock()
	fl.grant = Grant{Tokens: 0, RetryAfter: time.Hour}
	fl.mu.Unlock()
	later := start.Add(time.Second)
	fakeNow.Store(&later)
	if d := lm.Check("a", 1); d != Reject {
		t.Fatalf("check after expiry = %v, want Reject", d)
	}
}

func TestDrainedRecentGrantRejectsWithoutWaiting(t *testing.T) {
	fl := &fakeLeaser{grant: Grant{Tokens: 3, TTL: time.Minute}}
	lm := NewLimiter(fl)
	for i := 0; i < 3; i++ {
		if d := lm.Check("a", 1); d != Admit {
			t.Fatalf("check %d = %v, want Admit", i, d)
		}
	}
	// Block the next renewal; a drained-but-recent pool must reject
	// immediately rather than wait out the 25ms cold-start bound.
	fl.mu.Lock()
	fl.block = make(chan struct{})
	fl.mu.Unlock()
	start := time.Now()
	d := lm.Check("a", 1)
	elapsed := time.Since(start)
	close(fl.block)
	if d != Reject {
		t.Fatalf("drained check = %v, want Reject", d)
	}
	if elapsed >= 15*time.Millisecond {
		t.Fatalf("drained check took %v; must not wait on the renewal", elapsed)
	}
}

func TestCoordinatorDownBackoffLimitsCalls(t *testing.T) {
	fl := &fakeLeaser{err: context.DeadlineExceeded}
	lm := NewLimiter(fl)

	// Hammer for 300ms; backoff (min 250ms) must keep lease attempts to the
	// initial one plus at most a couple of retries.
	stop := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(stop) {
		if d := lm.Check("a", 1); d != Reject {
			t.Fatalf("check with dead coordinator = %v, want Reject (fail closed)", d)
		}
	}
	if got := fl.callCount(); got > 3 {
		t.Fatalf("lease attempts = %d, want <= 3 under backoff", got)
	}
}

func TestRecoversAfterCoordinatorReturns(t *testing.T) {
	fl := &fakeLeaser{err: context.DeadlineExceeded}
	lm := NewLimiter(fl)
	lm.Check("a", 1)
	waitFor(t, func() bool { return fl.callCount() >= 1 })

	fl.mu.Lock()
	fl.err = nil
	fl.grant = Grant{Tokens: 10, TTL: time.Minute}
	fl.mu.Unlock()

	waitFor(t, func() bool { return lm.Check("a", 1) == Admit })
}

func TestUnknownTenantNegativeCache(t *testing.T) {
	fl := &fakeLeaser{err: ErrUnknownTenant}
	lm := NewLimiter(fl)

	// The cold-start wait means even the first request sees the 404.
	if d := lm.Check("nosuch", 1); d != Unknown {
		t.Fatalf("first check = %v, want Unknown", d)
	}
	base := fl.callCount()
	for i := 0; i < 100; i++ {
		if d := lm.Check("nosuch", 1); d != Unknown {
			t.Fatalf("check = %v, want Unknown from negative cache", d)
		}
	}
	if fl.callCount() != base {
		t.Fatal("negative cache did not suppress lease calls")
	}
}

func TestWeightedCostAllOrNothing(t *testing.T) {
	fl := &fakeLeaser{grant: Grant{Tokens: 10, TTL: time.Minute}}
	lm := NewLimiter(fl)
	if d := lm.Check("a", 1); d != Admit { // pool 10 -> 9
		t.Fatalf("cost-1 check = %v, want Admit", d)
	}
	// Freeze further grants so token accounting is exact.
	fl.mu.Lock()
	fl.grant = Grant{Tokens: 0, RetryAfter: time.Hour}
	fl.mu.Unlock()

	seq := []struct {
		cost int
		want Decision
	}{
		{4, Admit},  // 9 -> 5
		{4, Admit},  // 5 -> 1
		{4, Reject}, // insufficient: 1 < 4, consumes nothing
		{1, Admit},  // 1 -> 0
		{1, Reject}, // empty
	}
	for i, s := range seq {
		if d := lm.Check("a", s.cost); d != s.want {
			t.Fatalf("step %d cost %d = %v, want %v", i, s.cost, d, s.want)
		}
	}
	if got := lm.counters("a").AdmittedTokens.Load(); got != 10 {
		t.Fatalf("admitted tokens = %d, want exactly the 10 granted", got)
	}
}

func TestInsufficientCostTriggersRenewalAboveWatermark(t *testing.T) {
	fl := &fakeLeaser{grant: Grant{Tokens: 3, TTL: time.Minute}}
	lm := NewLimiter(fl)
	if d := lm.Check("a", 1); d != Admit { // pool 3 -> 2, above watermark(1)
		t.Fatalf("cost-1 check = %v, want Admit", d)
	}
	base := fl.callCount()
	fl.mu.Lock()
	fl.grant = Grant{Tokens: 10, TTL: time.Minute}
	fl.mu.Unlock()

	// Pool holds 2 (>= watermark), but a cost-5 request cannot be satisfied:
	// it must still trigger a renewal or it would starve forever behind
	// steady low-cost traffic.
	lm.Check("a", 5)
	waitFor(t, func() bool { return fl.callCount() > base })
	waitFor(t, func() bool { return lm.Check("a", 5) == Admit })
}

func TestConcurrentAdmissionConservesTokens(t *testing.T) {
	// One grant of 10, then nothing: 200 racing requests must admit exactly
	// 10 in total — token accounting may never over-admit under concurrency.
	fl := &fakeLeaser{grant: Grant{Tokens: 10, TTL: time.Minute}}
	lm := NewLimiter(fl)
	if d := lm.Check("a", 1); d != Admit {
		t.Fatalf("first check = %v, want Admit", d)
	}
	fl.mu.Lock()
	fl.grant = Grant{Tokens: 0, RetryAfter: time.Hour}
	fl.mu.Unlock()

	var admits atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if lm.Check("a", 1) == Admit {
				admits.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := admits.Load() + 1; got != 10 { // +1 for the initial check
		t.Fatalf("total admits = %d, want exactly the 10 granted tokens", got)
	}
}

func TestZeroGrantHonorsRetryAfterPacing(t *testing.T) {
	fl := &fakeLeaser{grant: Grant{Tokens: 0, RetryAfter: time.Hour}}
	lm := NewLimiter(fl)

	lm.Check("a", 1)
	waitFor(t, func() bool { return fl.callCount() >= 1 })
	time.Sleep(10 * time.Millisecond) // let the zero-grant result land
	for i := 0; i < 100; i++ {
		lm.Check("a", 1)
	}
	if got := fl.callCount(); got != 1 {
		t.Fatalf("lease calls = %d, want 1 (paced by retryAfter)", got)
	}
}
