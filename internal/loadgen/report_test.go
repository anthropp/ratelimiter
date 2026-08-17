package loadgen

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anthropp/ratelimiter/internal/config"
)

func TestPercentile(t *testing.T) {
	r := &TenantResult{}
	for i := 1; i <= 100; i++ {
		r.latencies = append(r.latencies, float64(i))
	}
	if p := r.Percentile(50); p != 50 {
		t.Fatalf("p50 = %v, want 50", p)
	}
	if p := r.Percentile(99); p != 99 {
		t.Fatalf("p99 = %v, want 99", p)
	}
	if p := (&TenantResult{}).Percentile(99); p != 0 {
		t.Fatalf("empty p99 = %v, want 0", p)
	}
}

func TestWindowRate(t *testing.T) {
	r := &TenantResult{Buckets: []SecBucket{{Admitted: 10}, {Admitted: 20}, {Admitted: 30}}}
	adm := func(b SecBucket) int { return b.Admitted }
	if got := r.WindowRate(1, 3, adm); got != 25 {
		t.Fatalf("window [1,3) = %v, want 25", got)
	}
	if got := r.WindowRate(1, 10, adm); got != 25 { // `to` clamps to len
		t.Fatalf("clamped window = %v, want 25", got)
	}
	if got := r.WindowRate(5, 9, adm); got != 0 { // fully out of range
		t.Fatalf("out-of-range window = %v, want 0", got)
	}
}

// TestRenderChecksLastLineContract pins the exact final-line format that
// run.sh's exit code depends on: "PASS", or "FAIL: <names>".
func TestRenderChecksLastLineContract(t *testing.T) {
	lastLine := func(checks []Check) (string, bool) {
		rec := httptest.NewRecorder()
		pass := renderChecks(&Stream{w: rec, f: rec}, checks)
		lines := strings.Split(strings.TrimRight(rec.Body.String(), "\n"), "\n")
		return lines[len(lines)-1], pass
	}

	line, pass := lastLine([]Check{{Name: "a", OK: true}, {Name: "b", OK: true}})
	if line != "PASS" || !pass {
		t.Fatalf("all-pass last line = %q (pass=%v), want exactly \"PASS\"", line, pass)
	}
	line, pass = lastLine([]Check{{Name: "a", OK: false}, {Name: "b", OK: true}, {Name: "c", OK: false}})
	if line != "FAIL: a; c" || pass {
		t.Fatalf("failing last line = %q (pass=%v), want \"FAIL: a; c\"", line, pass)
	}
}

// TestSumSeriesRacesBucketGrowth hammers sumSeries against concurrent
// record() calls that keep growing the bucket array. Before the bounds check
// this panicked with index-out-of-range whenever an append landed between
// sumSeries's length measurement and its summing pass (crashed the loadgen
// mid-scenario on GKE once); run with -race.
func TestSumSeriesRacesBucketGrowth(t *testing.T) {
	rs := Results{"t": &TenantResult{Errors: map[string]int{}}}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for sec := 0; ; sec++ {
			select {
			case <-stop:
				return
			default:
				rs["t"].record(sec, 200, 1.0, nil, 1)
			}
		}
	}()
	for i := 0; i < 5000; i++ {
		sumSeries(rs, func(b SecBucket) int { return b.Admitted })
	}
	close(stop)
	<-done
}

func TestInvariantCheckBounds(t *testing.T) {
	cfg := &config.Config{Tenants: map[string]float64{"t": 100}}
	cfg.Lease.Size = 10
	env := &Env{Cfg: cfg}
	dur := 10 * time.Second
	// bound = 100*10 + burst 100 + workers(2) * 1.2 * leaseSize(10) = 1124.
	cases := []struct {
		tokens   int
		restarts int
		wantOK   bool
	}{
		{1124, 0, true},
		{1125, 0, false},
		{1224, 1, true}, // +1 coordinator restart adds one burst (100)
		{1225, 1, false},
	}
	for _, c := range cases {
		rs := Results{"t": &TenantResult{AdmittedTokens: c.tokens}}
		got := invariantCheck(env, rs, dur, 2, c.restarts)
		if got.OK != c.wantOK {
			t.Errorf("tokens=%d restarts=%d: OK=%v, want %v (%s)",
				c.tokens, c.restarts, got.OK, c.wantOK, got.Detail)
		}
	}
}

func TestInvariantCheckIgnoresUnconfiguredTenants(t *testing.T) {
	cfg := &config.Config{Tenants: map[string]float64{"t": 100}}
	cfg.Lease.Size = 10
	rs := Results{"ghost": &TenantResult{AdmittedTokens: 999999}}
	if got := invariantCheck(&Env{Cfg: cfg}, rs, time.Second, 2, 0); !got.OK {
		t.Fatalf("unconfigured tenant affected the invariant: %s", got.Detail)
	}
}
