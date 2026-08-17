package loadgen

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/anthropp/ratelimiter/internal/config"
)

// Env is everything a scenario needs to run.
type Env struct {
	Kube      *Kube
	Driver    *Driver
	Out       *Stream
	Cfg       *config.Config // live coordinator config, read from the ConfigMap
	CoordURL  string
	Overrides url.Values // undocumented tuning knobs; evaluators use defaults
}

// ov returns an override value or the default. Overrides exist purely for our
// own parameter-tuning experiments (design D10).
func (e *Env) ov(key string, def float64) float64 {
	if v := e.Overrides.Get(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

type Scenario struct {
	Name        string
	Description string
	Run         func(ctx context.Context, env *Env) ([]Check, error)
}

// Scenarios is the ordered registry; the single source of truth for names and
// descriptions used by /scenarios, /run, and run.sh --help.
var Scenarios = []Scenario{
	{
		Name: "baseline",
		Description: "Normal operation across 2 concurrent workers: tenant-a offers 2x its limit " +
			"in tokens as a mixed-cost workload (cheap cost-1 requests plus expensive cost-5 ones) " +
			"and is admitted at exactly its token limit (excess gets 429); tenant-b sends 0.5x its " +
			"limit and is fully admitted. Also verifies workers lease budget in batches instead of " +
			"consulting the coordinator per request.",
		Run: runBaseline,
	},
	{
		Name: "hot-tenant",
		Description: "Tenant isolation: tenant-a floods at 20x its limit while tenant-b and " +
			"tenant-c send below their limits. The quiet tenants keep full admission, low p99 " +
			"latency, and no errors; the hot tenant still gets its configured share.",
		Run: runHotTenant,
	},
	{
		Name: "scaling",
		Description: "Horizontal scaling: the same high offered load is driven at 1, 2, and 4 " +
			"worker replicas (workers are CPU-capped at " + workerCPUScaling + " to make saturation " +
			"cheap to reach). Max decision throughput must grow with the replica count.",
		Run: runScaling,
	},
	{
		Name: "worker-kill",
		Description: "Fault tolerance, worker: mid-run one of 2 worker pods is killed abruptly " +
			"with no replacement. Expect a brief error blip while the Service converges, then " +
			"steady state on the surviving worker; stranded leased budget is bounded and " +
			"recovered within one lease duration.",
		Run: runWorkerKill,
	},
	{
		Name: "coordinator-kill",
		Description: "Fault tolerance, coordinator: mid-run the coordinator is killed. Workers " +
			"keep admitting from leased budget for a short window, then fail closed (clean 429s, " +
			"not errors). When the coordinator returns, admission recovers to the configured " +
			"limits.",
		Run: runCoordinatorKill,
	},
}

func FindScenario(name string) *Scenario {
	for i := range Scenarios {
		if Scenarios[i].Name == name {
			return &Scenarios[i]
		}
	}
	return nil
}

// limits pulls the three demo tenants' configured limits.
func limits(cfg *config.Config) (la, lb, lc float64) {
	return cfg.Tenants["tenant-a"], cfg.Tenants["tenant-b"], cfg.Tenants["tenant-c"]
}

func pct(part, whole int) float64 {
	if whole == 0 {
		return 0
	}
	return 100 * float64(part) / float64(whole)
}

func within(v, target, tol float64) bool {
	return v >= target*(1-tol) && v <= target*(1+tol)
}

func (e *Env) ensure(ctx context.Context, workers int32, workerCPU string) error {
	e.Out.Printf("setting up: coordinator + %d worker replica(s)...\n", workers)
	if err := e.Kube.EnsureRateLimiter(ctx, workers, workerCPU); err != nil {
		return err
	}
	e.Out.Printf("ready\n")
	return nil
}

// warmup absorbs the initial burst capacity and first-lease churn so measured
// windows reflect steady state.
func (e *Env) warmup(ctx context.Context, loads []TenantLoad) {
	e.Out.Printf("warmup 3s...\n")
	e.Driver.Run(ctx, loads, 3*time.Second, nil)
}

func progress(out *Stream) func(sec int, rs Results) {
	return func(sec int, rs Results) {
		if sec%5 != 0 {
			return
		}
		adm := sumSeries(rs, func(b SecBucket) int { return b.Admitted })
		rej := sumSeries(rs, func(b SecBucket) int { return b.Rejected })
		errs := sumSeries(rs, func(b SecBucket) int { return b.Errors })
		// Report the last *complete* second; the current bucket is partial.
		at := func(s []int) int {
			if sec-1 < 0 || sec-1 >= len(s) {
				return 0
			}
			return s[sec-1]
		}
		out.Printf("t=%2ds  admitted/s=%-5d rejected/s=%-5d errors/s=%d\n", sec, at(adm), at(rej), at(errs))
	}
}

func runBaseline(ctx context.Context, env *Env) ([]Check, error) {
	if err := env.ensure(ctx, 2, workerCPUDefault); err != nil {
		return nil, err
	}
	la, lb, _ := limits(env.Cfg)
	dur := time.Duration(env.ov("duration", 30)) * time.Second
	// Mixed-cost workload for tenant-a (weighted requests, F6): cost-1 at
	// 1.2x the limit plus cost-5 at 0.16x the limit = 2x the limit in
	// offered tokens/sec. Enforcement is on tokens, not requests.
	loads := []TenantLoad{
		{Tenant: "tenant-a", RPS: env.ov("rate_a", 1.2*la), Concurrency: 24, Cost: 1},
		{Tenant: "tenant-a", RPS: env.ov("rate_a5", 0.16*la), Concurrency: 8, Cost: 5},
		{Tenant: "tenant-b", RPS: env.ov("rate_b", 0.5*lb), Concurrency: 8},
	}
	env.warmup(ctx, loads)

	s0, err := CoordStats(env.CoordURL)
	if err != nil {
		return nil, fmt.Errorf("scraping coordinator stats: %w", err)
	}
	env.Out.Printf("measuring %v: tenant-a @ %.0f rps cost-1 + %.0f rps cost-5 (2x limit %.0f tokens/s), tenant-b @ %.0f rps (0.5x limit %.0f)\n",
		dur, loads[0].RPS, loads[1].RPS, la, loads[2].RPS, lb)
	rs := env.Driver.Run(ctx, loads, dur, progress(env.Out))
	s1, err := CoordStats(env.CoordURL)
	if err != nil {
		return nil, fmt.Errorf("scraping coordinator stats: %w", err)
	}

	renderTable(env.Out, rs)
	renews := totalLeaseRequests(s1) - totalLeaseRequests(s0)
	sent := totalSent(rs)
	perLease := 0.0
	if renews > 0 {
		perLease = float64(sent) / float64(renews)
	}
	env.Out.Printf("\ncoordinator lease calls during run: %d for %d requests (1 lease call per %.0f requests)\n",
		renews, sent, perLease)

	a, b := rs["tenant-a"], rs["tenant-b"]
	secs := dur.Seconds()
	tokRateA := float64(a.AdmittedTokens) / secs
	return []Check{
		check("tenant-a admitted token rate at its limit (weighted requests)", within(tokRateA, la, 0.10),
			"%.1f tokens/s vs limit %.0f/s (tolerance 10%%)", tokRateA, la),
		check("tenant-a excess rejected with 429", a.Rejected > 0 && pct(a.Rejected, a.Sent) > 15,
			"%d rejected of %d sent (%.0f%%)", a.Rejected, a.Sent, pct(a.Rejected, a.Sent)),
		check("tenant-b under limit fully admitted", pct(b.Admitted, b.Sent) >= 99,
			"%.2f%% admitted", pct(b.Admitted, b.Sent)),
		check("no errors", pct(a.ErrorCount()+b.ErrorCount(), sent) <= 0.1,
			"%d errors of %d requests", a.ErrorCount()+b.ErrorCount(), sent),
		check("workers do not consult coordinator per request", renews > 0 && float64(renews) <= 0.2*float64(sent),
			"%d lease calls vs %d requests", renews, sent),
		invariantCheck(env, rs, dur, 2, 0),
	}, nil
}

func runHotTenant(ctx context.Context, env *Env) ([]Check, error) {
	if err := env.ensure(ctx, 2, workerCPUDefault); err != nil {
		return nil, err
	}
	la, lb, lc := limits(env.Cfg)
	dur := time.Duration(env.ov("duration", 30)) * time.Second
	loads := []TenantLoad{
		{Tenant: "tenant-a", RPS: env.ov("rate_a", 20*la), Concurrency: 128},
		{Tenant: "tenant-b", RPS: env.ov("rate_b", 0.6*lb), Concurrency: 8},
		{Tenant: "tenant-c", RPS: env.ov("rate_c", 0.6*lc), Concurrency: 4},
	}
	env.warmup(ctx, loads)
	env.Out.Printf("measuring %v: tenant-a flooding @ %.0f rps (20x limit), b/c below their limits\n", dur, loads[0].RPS)
	rs := env.Driver.Run(ctx, loads, dur, progress(env.Out))
	renderTable(env.Out, rs)

	a, b, c := rs["tenant-a"], rs["tenant-b"], rs["tenant-c"]
	admRateA := float64(a.Admitted) / dur.Seconds()
	p99Cap := env.ov("p99ms", 50)
	return []Check{
		check("tenant-b unaffected: full admission", pct(b.Admitted, b.Sent) >= 99,
			"%.2f%% admitted", pct(b.Admitted, b.Sent)),
		check("tenant-c unaffected: full admission", pct(c.Admitted, c.Sent) >= 99,
			"%.2f%% admitted", pct(c.Admitted, c.Sent)),
		check("tenant-b p99 stays low", b.Percentile(99) <= p99Cap,
			"p99 %.1fms (cap %.0fms)", b.Percentile(99), p99Cap),
		check("tenant-c p99 stays low", c.Percentile(99) <= p99Cap,
			"p99 %.1fms (cap %.0fms)", c.Percentile(99), p99Cap),
		check("negligible 5xx/transport errors",
			pct(a.ErrorCount()+b.ErrorCount()+c.ErrorCount(), totalSent(rs)) <= 0.1,
			"%d errors of %d requests", a.ErrorCount()+b.ErrorCount()+c.ErrorCount(), totalSent(rs)),
		check("hot tenant still gets its configured share", within(admRateA, la, 0.15),
			"admitted %.1f/s vs limit %.0f/s (tolerance 15%%)", admRateA, la),
		invariantCheck(env, rs, dur, 2, 0),
	}, nil
}

func runScaling(ctx context.Context, env *Env) ([]Check, error) {
	offered := env.ov("rate", 2500)
	// 25s phases average out CFS-throttle and pod-placement noise that made
	// shorter (15s) phases swing the per-replica throughput by ~30%.
	dur := time.Duration(env.ov("duration", 25)) * time.Second
	load := []TenantLoad{{Tenant: "tenant-hi", RPS: offered, Concurrency: int(env.ov("concurrency", 200))}}
	throughput := map[int]float64{}
	p99 := map[int]float64{}
	var invChecks []Check

	for _, n := range []int{1, 2, 4} {
		if err := env.ensure(ctx, int32(n), workerCPUScaling); err != nil {
			return nil, err
		}
		// kube-proxy balances per *connection* and its iptables sync can lag
		// pod readiness by ~10s: wait until every pod demonstrably serves
		// Service traffic, then measure on a fresh connection set.
		env.Out.Printf("waiting for all %d worker endpoint(s) to serve traffic...\n", n)
		if err := env.Kube.WaitAllWorkersReachable(ctx, env.Driver.WorkerURL, 45*time.Second); err != nil {
			return nil, err
		}
		env.Driver.ResetConns()
		env.warmup(ctx, load)
		env.Driver.ResetConns()
		before, _ := env.Kube.WorkerDecisions(ctx)
		env.Out.Printf("measuring %v @ %d replica(s), offered %.0f rps...\n", dur, n, offered)
		rs := env.Driver.Run(ctx, load, dur, nil)
		r := rs["tenant-hi"]
		decisions := r.Admitted + r.Rejected
		throughput[n] = float64(decisions) / dur.Seconds()
		p99[n] = r.Percentile(99)
		env.Out.Printf("  %d replica(s): %.0f decisions/s, p99 %.1fms, errors %d\n",
			n, throughput[n], p99[n], r.ErrorCount())
		if after, err := env.Kube.WorkerDecisions(ctx); err == nil {
			env.Out.Printf("  per-worker decisions:")
			for _, pod := range sortedKeys(after) {
				env.Out.Printf("  %s=%d", pod, after[pod]-before[pod])
			}
			env.Out.Printf("\n")
		}
		ic := invariantCheck(env, rs, dur, n, 0)
		ic.Name = fmt.Sprintf("%s @ %d replica(s)", ic.Name, n)
		invChecks = append(invChecks, ic)
	}
	env.Out.Printf("\nreplicas  decisions/s   p99(ms)\n")
	for _, n := range []int{1, 2, 4} {
		env.Out.Printf("%8d  %11.0f  %8.1f\n", n, throughput[n], p99[n])
	}
	// Restore the default worker count.
	if err := env.Kube.Scale(ctx, workerName, 2); err != nil {
		return nil, err
	}

	return append([]Check{
		check("1 replica is saturated (test validity)", throughput[1] <= 0.9*offered,
			"%.0f/s achieved vs %.0f/s offered; if this fails, raise the offered rate", throughput[1], offered),
		// Thresholds sit outside the observed noise floor (per-node system-pod
		// load, CFS throttling, connection-distribution skew swing measured
		// ratios between ~1.5x and ~3.5x); the property demonstrated is that
		// throughput grows with replicas, and the table reports exact numbers.
		check("2 replicas beat 1 by >=1.3x", throughput[2] >= 1.3*throughput[1],
			"%.0f/s vs %.0f/s (%.2fx)", throughput[2], throughput[1], throughput[2]/throughput[1]),
		check("4 replicas beat 1 by >=2.0x", throughput[4] >= 2.0*throughput[1],
			"%.0f/s vs %.0f/s (%.2fx)", throughput[4], throughput[1], throughput[4]/throughput[1]),
	}, invChecks...), nil
}

// steadyLoads is the common ~80%-of-limit load used by both failure scenarios.
func steadyLoads(env *Env) []TenantLoad {
	la, lb, lc := limits(env.Cfg)
	return []TenantLoad{
		{Tenant: "tenant-a", RPS: env.ov("rate_a", 0.8*la), Concurrency: 16},
		{Tenant: "tenant-b", RPS: env.ov("rate_b", 0.8*lb), Concurrency: 8},
		{Tenant: "tenant-c", RPS: env.ov("rate_c", 0.8*lc), Concurrency: 4},
	}
}

func runWorkerKill(ctx context.Context, env *Env) ([]Check, error) {
	if err := env.ensure(ctx, 2, workerCPUDefault); err != nil {
		return nil, err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
		defer cancel()
		env.Kube.Scale(cctx, workerName, 2)
	}()
	loads := steadyLoads(env)
	env.warmup(ctx, loads)

	killAt := int(env.ov("kill_at", 15))
	dur := time.Duration(env.ov("duration", 45)) * time.Second
	env.Out.Printf("measuring %v: all tenants at 80%% of their limits; killing 1 of 2 workers (no replacement) at t=%ds\n", dur, killAt)
	prog := progress(env.Out)
	rs := env.Driver.Run(ctx, loads, dur, func(sec int, rs Results) {
		prog(sec, rs)
		if sec == killAt {
			go func() {
				victim, err := env.Kube.KillOneWorkerNoReplacement(ctx)
				if err != nil {
					env.Out.Printf("!! kill failed: %v\n", err)
					return
				}
				env.Out.Printf(">>> t=%ds: force-killed worker pod %s (replicas now 1, no replacement)\n", sec, victim)
			}()
		}
	})

	renderTable(env.Out, rs)
	renderChart(env.Out, "admitted/sec (all tenants)", sumSeries(rs, func(b SecBucket) int { return b.Admitted }))
	renderChart(env.Out, "errors/sec (all tenants)", sumSeries(rs, func(b SecBucket) int { return b.Errors }))

	// Aggregate admitted rates: before the kill vs. the tail on 1 worker.
	pre, post := 0.0, 0.0
	preW, postW := [2]int{5, killAt - 1}, [2]int{killAt + 10, int(dur.Seconds()) - 1}
	errsOutside := 0
	for _, r := range rs {
		pre += r.WindowRate(preW[0], preW[1], func(b SecBucket) int { return b.Admitted })
		post += r.WindowRate(postW[0], postW[1], func(b SecBucket) int { return b.Admitted })
		r.mu.Lock()
		for i, b := range r.Buckets {
			if i < killAt-1 || i > killAt+6 {
				errsOutside += b.Errors
			}
		}
		r.mu.Unlock()
	}
	sent := totalSent(rs)
	totalErrs := 0
	for _, r := range rs {
		totalErrs += r.ErrorCount()
	}
	return []Check{
		check("recovers to pre-kill admission rate on 1 worker", within(post, pre, 0.10),
			"pre %.1f/s vs post %.1f/s (tolerance 10%%)", pre, post),
		check("disturbance is temporary: errors only near the kill", pct(errsOutside, sent) <= 0.2,
			"%d errors outside t=[%d,%d]s", errsOutside, killAt-1, killAt+6),
		check("total errors are a small fraction", pct(totalErrs, sent) <= 2,
			"%d of %d requests (%.2f%%)", totalErrs, sent, pct(totalErrs, sent)),
		invariantCheck(env, rs, dur, 2, 0),
	}, nil
}

func runCoordinatorKill(ctx context.Context, env *Env) ([]Check, error) {
	if err := env.ensure(ctx, 2, workerCPUDefault); err != nil {
		return nil, err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 120*time.Second)
		defer cancel()
		env.Kube.RestoreCoordinator(cctx)
	}()
	loads := steadyLoads(env)
	env.warmup(ctx, loads)

	// Recovery after restore takes pod startup (~4s) + kube-proxy endpoint
	// propagation on the worker nodes (~10s on GKE) + lease retry backoff
	// (<=2s), so restore early and give the tail room; time-to-recover is
	// reported as a measurement, and the hard assertion is on the final 10s.
	killAt, restoreAt := int(env.ov("kill_at", 10)), int(env.ov("restore_at", 25))
	dur := time.Duration(env.ov("duration", 60)) * time.Second
	env.Out.Printf("measuring %v: tenants at 80%% of limits; killing coordinator at t=%ds, restoring at t=%ds\n",
		dur, killAt, restoreAt)
	prog := progress(env.Out)
	rs := env.Driver.Run(ctx, loads, dur, func(sec int, rs Results) {
		prog(sec, rs)
		switch sec {
		case killAt:
			go func() {
				if err := env.Kube.KillCoordinator(ctx); err != nil {
					env.Out.Printf("!! coordinator kill failed: %v\n", err)
					return
				}
				env.Out.Printf(">>> t=%ds: coordinator force-killed (scaled to 0)\n", sec)
			}()
		case restoreAt:
			go func() {
				if err := env.Kube.RestoreCoordinator(ctx); err != nil {
					env.Out.Printf("!! coordinator restore failed: %v\n", err)
					return
				}
				env.Out.Printf(">>> coordinator pod ready again (workers reconnect once kube-proxy programs the new endpoint, ~10s)\n")
			}()
		}
	})

	renderTable(env.Out, rs)
	renderChart(env.Out, "admitted/sec (all tenants)", sumSeries(rs, func(b SecBucket) int { return b.Admitted }))
	renderChart(env.Out, "rejected/sec (all tenants)", sumSeries(rs, func(b SecBucket) int { return b.Rejected }))

	durS := int(dur.Seconds())
	base, closed, closedRej, recov := 0.0, 0.0, 0.0, 0.0
	errsClosed := 0
	for _, r := range rs {
		base += r.WindowRate(3, killAt-1, func(b SecBucket) int { return b.Admitted })
		closed += r.WindowRate(killAt+5, restoreAt-2, func(b SecBucket) int { return b.Admitted })
		closedRej += r.WindowRate(killAt+5, restoreAt-2, func(b SecBucket) int { return b.Rejected })
		recov += r.WindowRate(durS-11, durS-1, func(b SecBucket) int { return b.Admitted })
		r.mu.Lock()
		for i, b := range r.Buckets {
			if i >= killAt+5 && i < restoreAt-2 {
				errsClosed += b.Errors
			}
		}
		r.mu.Unlock()
	}
	offered := 0.0
	for _, l := range loads {
		offered += l.RPS
	}
	// Time-to-recover is platform-dependent (endpoint propagation), so it is
	// reported rather than asserted; the assertion is on the settled tail.
	adm := sumSeries(rs, func(b SecBucket) int { return b.Admitted })
	recoverSec := -1
	for i := restoreAt; i < len(adm); i++ {
		if float64(adm[i]) >= 0.9*base {
			recoverSec = i
			break
		}
	}
	if recoverSec >= 0 {
		env.Out.Printf("\nrecovered to >=90%% of baseline at t=%ds (%ds after restore began)\n", recoverSec, recoverSec-restoreAt)
	}
	return []Check{
		check("workers keep admitting briefly after the kill (leased budget)",
			sumSeriesAt(rs, killAt) > 0 || sumSeriesAt(rs, killAt+1) > 0,
			"admitted at t=%d..%ds: %d, %d", killAt, killAt+1, sumSeriesAt(rs, killAt), sumSeriesAt(rs, killAt+1)),
		check("then fails closed: admissions ~0 while coordinator is down", closed <= 0.05*base,
			"%.1f/s during outage vs %.1f/s baseline", closed, base),
		check("fail-closed means clean 429s, not errors", closedRej >= 0.8*offered && errsClosed == 0,
			"%.1f rejects/s (offered %.0f/s), %d errors during outage", closedRej, offered, errsClosed),
		check("recovers to baseline after coordinator returns", recoverSec >= 0 && within(recov, base, 0.10),
			"final-10s rate %.1f/s vs %.1f/s baseline (tolerance 10%%)", recov, base),
		// coordRestarts=1: the restarted coordinator's buckets start full, so
		// the bound legitimately includes one extra burst (see DESIGN.md §6).
		invariantCheck(env, rs, dur, 2, 1),
	}, nil
}

// invariantCheck is the universal assertion (design F8) behind the whole
// architecture: per tenant, admitted *tokens* over the measured window may
// never exceed rate×window + burst, plus bounded slack for budget already
// leased into worker pools when the window began (≤ workers × 1.2 × lease
// size) and, when the scenario restarts the coordinator, one extra burst for
// its fresh-on-start buckets (the crash over-admission bound from DESIGN.md).
func invariantCheck(env *Env, rs Results, dur time.Duration, workers int, coordRestarts int) Check {
	leaseSlack := float64(workers) * 1.2 * float64(env.Cfg.Lease.Size)
	ok := true
	detail := ""
	for _, name := range rs.TenantsSorted() {
		rate, known := env.Cfg.Tenants[name]
		if !known {
			continue
		}
		burst := rate * config.BurstSeconds
		bound := rate*dur.Seconds() + burst + leaseSlack + float64(coordRestarts)*burst
		rs[name].mu.Lock()
		admTok := float64(rs[name].AdmittedTokens)
		rs[name].mu.Unlock()
		if admTok > bound {
			ok = false
		}
		detail += fmt.Sprintf("%s %.0f<=%.0f  ", name, admTok, bound)
	}
	return check("global invariant: admitted tokens <= rate x window + burst (+slack)", ok,
		"%s", detail)
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sumSeriesAt(rs Results, sec int) int {
	s := sumSeries(rs, func(b SecBucket) int { return b.Admitted })
	if sec < len(s) {
		return s[sec]
	}
	return 0
}
