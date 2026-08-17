package loadgen

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"
)

func intstrFromInt(port int32) intstr.IntOrString { return intstr.FromInt32(port) }

// TenantLoad describes offered load for one tenant. Cost is the per-request
// token cost (weighted rate limiting, design F6); 0 means 1. Multiple loads
// may target the same tenant (e.g. a mixed-cost workload) — their results
// merge into one TenantResult.
type TenantLoad struct {
	Tenant      string
	RPS         float64
	Concurrency int
	Cost        int
}

// SecBucket is one second of per-tenant outcome counts, for time-series charts.
type SecBucket struct {
	Admitted, Rejected, Errors int
}

// TenantResult accumulates client-observed outcomes for one tenant.
type TenantResult struct {
	mu             sync.Mutex
	Sent           int
	Admitted       int // HTTP 200
	AdmittedTokens int // sum of admitted requests' costs (= Admitted at cost 1)
	Rejected       int // HTTP 429
	ClientDrops    int            // dispatch skipped: all Concurrency slots busy
	Errors         map[string]int // other HTTP codes and transport errors
	latencies      []float64      // milliseconds, admitted+rejected responses
	Buckets        []SecBucket    // indexed by whole seconds since driver start
}

func (r *TenantResult) record(sec int, code int, latMs float64, transportErr error, cost int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.Buckets) <= sec {
		r.Buckets = append(r.Buckets, SecBucket{})
	}
	b := &r.Buckets[sec]
	switch {
	case transportErr != nil:
		r.Errors["transport"]++
		b.Errors++
	case code == http.StatusOK:
		r.Admitted++
		r.AdmittedTokens += cost
		b.Admitted++
		r.latencies = append(r.latencies, latMs)
	case code == http.StatusTooManyRequests:
		r.Rejected++
		b.Rejected++
		r.latencies = append(r.latencies, latMs)
	default:
		r.Errors[fmt.Sprintf("%d", code)]++
		b.Errors++
	}
}

// Percentile returns the p-th percentile latency in ms (p in [0,100]).
func (r *TenantResult) Percentile(p float64) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.latencies) == 0 {
		return 0
	}
	sorted := make([]float64, len(r.latencies))
	copy(sorted, r.latencies)
	sort.Float64s(sorted)
	idx := int(p / 100 * float64(len(sorted)-1))
	return sorted[idx]
}

func (r *TenantResult) ErrorCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, v := range r.Errors {
		n += v
	}
	return n
}

// WindowRate returns the mean per-second count of the chosen metric over
// bucket seconds [from, to).
func (r *TenantResult) WindowRate(from, to int, metric func(SecBucket) int) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if to > len(r.Buckets) {
		to = len(r.Buckets)
	}
	if from >= to {
		return 0
	}
	sum := 0
	for _, b := range r.Buckets[from:to] {
		sum += metric(b)
	}
	return float64(sum) / float64(to-from)
}

// Results maps tenant name to its result. Ordered helpers keep output stable.
type Results map[string]*TenantResult

func (rs Results) TenantsSorted() []string {
	names := make([]string, 0, len(rs))
	for n := range rs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Driver generates paced load against the worker service.
type Driver struct {
	WorkerURL string // e.g. http://ratelim-workers:8080
	client    *http.Client
}

func NewDriver(workerURL string) *Driver {
	return &Driver{
		WorkerURL: workerURL,
		client: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        4096,
				MaxIdleConnsPerHost: 4096,
			},
		},
	}
}

// ResetConns closes idle keep-alive connections. kube-proxy balances per
// connection, so after scaling the worker deployment the old connection set
// still points at the old pods; dropping it forces a fresh, evenly
// distributed set across current endpoints.
func (d *Driver) ResetConns() {
	d.client.CloseIdleConnections()
}

// Run drives all tenant loads concurrently for the given duration.
// onSecond, if non-nil, is called once per elapsed second with the live
// results (for progress output and mid-run fault injection hooks).
func (d *Driver) Run(ctx context.Context, loads []TenantLoad, duration time.Duration, onSecond func(sec int, rs Results)) Results {
	results := Results{}
	for _, l := range loads {
		results[l.Tenant] = &TenantResult{Errors: map[string]int{}}
	}
	start := time.Now()
	var wg sync.WaitGroup

	if onSecond != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for sec := 1; ; sec++ {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if time.Since(start) >= duration {
						return
					}
					onSecond(sec, results)
				}
			}
		}()
	}

	for _, l := range loads {
		wg.Add(1)
		go func(l TenantLoad) {
			defer wg.Done()
			d.runTenant(ctx, l, results[l.Tenant], start, duration)
		}(l)
	}
	wg.Wait()
	return results
}

// runTenant paces dispatch with a 5ms accumulator loop: each tick releases
// floor(accumulated quota) requests, which stays accurate at high RPS where
// per-request sleeps would not.
func (d *Driver) runTenant(ctx context.Context, l TenantLoad, res *TenantResult, start time.Time, duration time.Duration) {
	sem := make(chan struct{}, l.Concurrency)
	var wg sync.WaitGroup
	cost := l.Cost
	if cost < 1 {
		cost = 1
	}
	url := fmt.Sprintf("%s/v1/check/%s", d.WorkerURL, l.Tenant)
	if cost > 1 {
		url += fmt.Sprintf("?cost=%d", cost)
	}

	quota := 0.0
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case now := <-tick.C:
			if now.Sub(start) >= duration {
				wg.Wait()
				return
			}
			quota += now.Sub(last).Seconds() * l.RPS
			last = now
			n := int(quota)
			quota -= float64(n)
			for i := 0; i < n; i++ {
				select {
				case sem <- struct{}{}:
					wg.Add(1)
					go func() {
						defer wg.Done()
						defer func() { <-sem }()
						d.one(ctx, url, cost, res, start)
					}()
				default:
					res.mu.Lock()
					res.ClientDrops++
					res.mu.Unlock()
				}
			}
		}
	}
}

func (d *Driver) one(ctx context.Context, url string, cost int, res *TenantResult, start time.Time) {
	res.mu.Lock()
	res.Sent++
	res.mu.Unlock()
	t0 := time.Now()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := d.client.Do(req)
	lat := time.Since(t0)
	sec := int(t0.Sub(start).Seconds())
	if err != nil {
		if ctx.Err() != nil {
			return // shutdown, not a system error
		}
		res.record(sec, 0, 0, err, cost)
		return
	}
	resp.Body.Close()
	res.record(sec, resp.StatusCode, float64(lat.Microseconds())/1000, nil, cost)
}
