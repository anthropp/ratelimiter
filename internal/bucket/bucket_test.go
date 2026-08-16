package bucket

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestStartsFullAndDrains(t *testing.T) {
	b := New(10, 100, t0)
	if got := b.TakeUpTo(60, t0); got != 60 {
		t.Fatalf("first take = %d, want 60", got)
	}
	if got := b.TakeUpTo(60, t0); got != 40 {
		t.Fatalf("second take = %d, want remaining 40", got)
	}
	if got := b.TakeUpTo(60, t0); got != 0 {
		t.Fatalf("empty take = %d, want 0", got)
	}
}

func TestRefillRateAndCap(t *testing.T) {
	b := New(10, 100, t0)
	b.TakeUpTo(100, t0) // drain

	// 2.5s at 10/s -> 25 tokens.
	if got := b.TakeUpTo(1000, t0.Add(2500*time.Millisecond)); got != 25 {
		t.Fatalf("refilled take = %d, want 25", got)
	}
	// After a long idle period the bucket must not exceed capacity.
	if got := b.TakeUpTo(1000, t0.Add(time.Hour)); got != 100 {
		t.Fatalf("capped take = %d, want 100", got)
	}
}

func TestGrantedOverWindowNeverExceedsRatePlusBurst(t *testing.T) {
	// The global invariant: however aggressively we ask, total grants over a
	// window are <= rate*window + capacity.
	b := New(50, 50, t0)
	total := 0
	for ms := 0; ms <= 10000; ms += 7 {
		total += b.TakeUpTo(13, t0.Add(time.Duration(ms)*time.Millisecond))
	}
	max := 50*10 + 50
	if total > max {
		t.Fatalf("granted %d over 10s, invariant max %d", total, max)
	}
	if total < 500 { // sanity: should get close to rate*window
		t.Fatalf("granted %d over 10s, expected ~%d", total, max)
	}
}

func TestNextTokenIn(t *testing.T) {
	b := New(10, 100, t0)
	b.TakeUpTo(100, t0)
	if d := b.NextTokenIn(t0); d != 100*time.Millisecond {
		t.Fatalf("NextTokenIn = %v, want 100ms", d)
	}
	if d := b.NextTokenIn(t0.Add(time.Second)); d != 0 {
		t.Fatalf("NextTokenIn after refill = %v, want 0", d)
	}
}

func TestClockGoingBackwardsIsIgnored(t *testing.T) {
	b := New(10, 100, t0)
	b.TakeUpTo(100, t0.Add(time.Second))
	if got := b.TakeUpTo(100, t0); got != 0 {
		t.Fatalf("take with earlier now = %d, want 0 (no refill)", got)
	}
}
