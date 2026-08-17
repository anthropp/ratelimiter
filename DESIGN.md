# Distributed Rate Limiter — Design

Take-home project. Requirements: `dist-ratelim.txt`. Budget: ~6 hours to design, build, deploy (GKE), and test.

## 1. Goal and non-goals

**Goal:** a lease-based distributed rate limiter — one coordinator holding global per-tenant limits, N worker replicas making local admission decisions from leased budget — plus a load generator / evaluation harness that deploys the system, drives load, injects failures, and reports results. Demonstrates: correct enforcement under concurrency, tenant isolation, horizontal scaling, graceful degradation when workers or the coordinator die (fail-closed).

**Non-goals:** persistence (all state in-memory except the ConfigMap), authn/authz, multi-region, config hot-reload, exactly-once accounting across crashes (over-admission after a crash is explicitly acceptable).

## 2. System overview

```
                       ┌────────────────────────────────────────────┐
 evaluator laptop ──►  │  loadgen (1 pod, LoadBalancer Service)     │
 curl /run?scenario=…  │  • scenario engine  • k8s client (RBAC)    │
                       │  • load driver      • metrics + report     │
                       └───────┬───────────────────────┬────────────┘
                               │ admission traffic     │ k8s API: create/scale/kill
                               ▼                       ▼
                    ┌─────────────────────┐   ┌──────────────────────┐
                    │ worker Service      │   │ coordinator (1 pod)  │
                    │  worker pod × N     │──►│  per-tenant global   │
                    │  local token pools  │   │  token buckets       │
                    │  GET /v1/check/{t}  │   │  POST /v1/lease      │
                    └─────────────────────┘   └───────────▲──────────┘
                                                          │ ConfigMap: lease size,
                                                          │ lease duration, tenant limits
```

All three components are one Go binary (`ratelim`) with subcommands `coordinator`, `worker`, `loadgen`; one container image. Plain HTTP + JSON everywhere (no gRPC — not worth the toolchain time at this scale).

## 3. Rate-limiting model

**Global accounting (coordinator).** Per tenant, a token bucket: refill rate = configured limit `R` tokens/sec, capacity `B = R × 1s` (one second of burst; see decision D6). A lease request debits `min(leaseSize, available)` tokens from the bucket and returns that grant. **Grants are debits, not tracked leases** — the coordinator keeps no per-worker or per-lease state, only the buckets and counters. Consequences:

- Global invariant: tokens granted over any window ≤ `R × window + B`, so admitted ≤ that too. The limit cannot be exceeded in steady state regardless of worker count.
- Unused tokens on a crashed/idle worker are simply lost (slight under-admission, bounded by ~2×leaseSize per worker per tenant, self-healing within one lease duration). We don't reclaim expired leases — that would require lease tracking for marginal benefit.

**Local admission (worker).** Per tenant, a pool of leased tokens with an expiry (`grantTime + leaseDuration`; enforced with the worker's monotonic clock — no cross-node clock dependency). Admission check: pool > 0 → decrement, admit (200); pool empty → reject (429) immediately. Never blocks on the coordinator in the request path.

**Leasing (worker → coordinator).**
- On demand: first request for a tenant, and whenever the pool empties, triggers a lease request (singleflight per tenant — concurrent exhausted requests don't stampede).
- Prefetch: when the pool drops below 20% of the last grant, renew in the background. This keeps spurious 429s at the limit boundary negligible while preserving the headline property: coordinator sees ~`traffic / leaseSize` renewals, not one call per request.
- Zero-grant response carries `retryAfterMs`; the worker stays empty (429s) and retries then.
- Coordinator unreachable: retry with exponential backoff + jitter (250ms → 2s cap). Worker keeps admitting from remaining local tokens, then fails closed per tenant. Recovers on first successful renewal after the coordinator returns.

**Demand-driven balance.** The kube Service spreads a tenant's requests across workers; each worker leases only as its own demand requires, so skewed load self-corrects (the busy worker just renews more often).

## 4. Component details

### Coordinator
- State: `map[tenant]*bucket` (float tokens, lastRefill), atomic counters (lease requests, tokens granted, per tenant). Single mutex per tenant.
- Config from ConfigMap (mounted file, read at startup only; edit → restart pod to apply):

```yaml
lease:
  size: 10        # tokens per grant (global — same for every <tenant,worker>)
  durationMs: 2000
tenants:
  tenant-a: 100   # req/s
  tenant-b: 50
  tenant-c: 20
```

- API:
  - `POST /v1/lease` `{"tenant":"tenant-a","worker":"<pod-name>"}` → `{"granted":10,"ttlMs":2000}`; `granted:0` includes `retryAfterMs`; unknown tenant → 404.
  - `GET /v1/stats` → per-tenant lease-request and tokens-granted counters (loadgen scrapes start/end deltas to report renewal counts).
  - `GET /healthz`.

### Worker
- State: `map[tenant]{tokens int, expiry time, lastGrant int, renewing bool}`, per-tenant mutex.
- API: `GET /v1/check/{tenant}?cost=N` → 200 admitted / 429 rejected / 404 unknown tenant (negative-cached 10s after a coordinator 404); `cost` (default 1, range 1–1000) is the request's token cost — weighted rate limiting (F6), all-or-nothing (a pool below `cost` rejects and consumes nothing, and an unsatisfiable cost triggers a renewal even above the prefetch watermark so high-cost requests can't starve behind low-cost traffic); `GET /v1/stats`; `GET /healthz`.
- Readiness is HTTP-up only — a worker with a dead coordinator is still "ready" (it serves fail-closed decisions by design).
- CPU request/limit set low (~200m) so one loadgen can saturate workers for the scaling scenario.

### Load generator / evaluation harness
One pod, exposed via a `LoadBalancer` Service so the evaluator can drive it from a laptop with nothing but `curl` (no kubectl). `/run` requires a bearer token that is hard-coded into both the loadgen and `run.sh` (and therefore checked into the repo — demo-grade auth whose only job is keeping random internet scanners from triggering pod kills):

- `GET /scenarios` — machine-readable list of scenario names + descriptions (single source of truth: the loadgen's scenario registry).
- `GET /run?scenario=<name>` — runs the scenario, **streaming** chunked plaintext so the evaluator watches live: the scenario description first, then progress, the final report, and a last line reading `PASS` or `FAIL: <failed checks>`. Per-tenant rates and concurrency are hard-coded per scenario — the evaluator gets no load knobs to misconfigure; optional undocumented query overrides exist for our own tuning experiments. One run at a time (409 if busy).
- `run.sh <scenario>` is the evaluator CLI: `run.sh --help` fetches `/scenarios` and prints names + descriptions; otherwise it streams `/run` and exits 0 on `PASS`, 1 on `FAIL`.

**Kubernetes control (client-go, in-cluster config).** Per the spec, the loadgen owns the rate limiter's lifecycle: at scenario start it creates-or-updates the coordinator and worker Deployments/Services from embedded specs, scales workers to what the scenario needs, waits for readiness, and restores steady state afterwards. Failure injection = scaling Deployments and force-deleting pods (`gracePeriodSeconds=0` for abrupt death). RBAC Role scoped to the `ratelimiter` namespace: pods get/list/delete; deployments/services get/create/patch; deployments/scale update.

**Load driver.** Per tenant: paced dispatch at the target rate (ticker), in-flight bounded by `concurrency` (semaphore; a full semaphore at fire time counts as client-drop, expected ≈0 since 429s are fast). Client timeout 2s, high `MaxIdleConnsPerHost`. Records per request: tenant, HTTP code (or transport error), latency.

**Reporting.** Per tenant: sent, admitted (200), rejected (429), errors by code/class, p50/p99 latency (exact — latencies kept in memory and sorted; fine at this scale), plus coordinator lease-renewal delta and the renewals-to-requests ratio. Failure scenarios additionally bucket per-second admitted/rejected/error counts and render an ASCII time-series chart in the streamed output. Each scenario ends by evaluating hard-coded assertions over these metrics (per-scenario criteria in §5) and emits `PASS` or `FAIL: <reasons>` as the final line.

## 5. Scenarios (hard-coded)

| name | setup | load | what it demonstrates | ~time |
|---|---|---|---|---|
| `baseline` | 2 workers | tenant-a at 2× limit, tenant-b at ½ limit, 30s | a admitted ≈ limit ±5%, excess 429'd; b ≈ 100% admitted; renewals ≪ requests; enforcement correct across concurrent workers | 30s |
| `hot-tenant` | 2 workers | tenant-a at 20× limit + high concurrency; b, c below limit | b/c p99 stays low, admission correct, ~0 5xx | 30s |
| `scaling` | workers = 1, 2, 4 in sequence | fixed high offered load against a high-limit tenant | max decision throughput grows with replicas (table: replicas → achieved rps, p99) | ~3 min |
| `worker-kill` | 2 workers | all tenants at ~80% limit; at t=15s scale to 1 **and** force-delete the doomed pod (no replacement) | brief error/latency blip, ≤ ~2×leaseSize/tenant stranded, steady state on 1 worker; time-series chart | 45s |
| `coordinator-kill` | 2 workers | tenants at ~80% limit; t=10s scale coordinator→0 (force-delete); t=25s scale back to 1 | admissions continue seconds-long on local budget, then fail-closed 429s, full recovery after restart; time-series chart | 45s |

**Pass criteria** (asserted at end of run; thresholds below are initial values, to be tuned once deployed):

- `baseline` — tenant-a offers 2× its limit in *tokens* as a mixed-cost workload (cost-1 at 1.2× + cost-5 at 0.16× the limit; weighted requests, F6) and its admitted **token** rate must be within ±10% of the limit; tenant-b ≥ 99% admitted; 5xx ≈ 0; coordinator renewals ≤ 20% of requests sent (demonstrating ≪ 1 coordinator call per request).
- **Every scenario** additionally asserts the F8 global invariant: per tenant, admitted tokens ≤ rate × window + burst, plus bounded slack for budget already leased into worker pools at window start (workers × 1.2 × lease size) and, in coordinator-kill, one extra burst for the restarted coordinator's fresh buckets.
- `hot-tenant` — tenant-b/c ≥ 99% admitted with p99 ≤ 50 ms; 5xx ≤ 0.1% for all tenants.
- `scaling` — throughput(2 workers) ≥ 1.3 × throughput(1); throughput(4) ≥ 2.0 × throughput(1). (Thresholds sit outside the measured noise floor — observed ratios range ~1.5–3.5× — while the report prints exact numbers; see §11a.)
- `worker-kill` — errors confined to a ≤ 5 s window around the kill; steady-state admitted rate after the kill within ±10% of before; overall 5xx ≤ 1%.
- `coordinator-kill` — after the kill, admitted rate decays to ~0 within a few seconds and rejections are clean 429s (fail-closed, not errors); within 10 s of restart, admitted recovers to within ±10% of the pre-kill rate.

## 6. Failure behavior summary

| event | behavior | bound |
|---|---|---|
| worker crash | other workers unaffected; its unused tokens lost | ≤ ~2×leaseSize per tenant, one-time; Service endpoint lag ~seconds |
| coordinator down | workers admit from local pools, then fail closed per tenant; retry with backoff | admissions continue for roughly `localTokens / tenantRate` seconds |
| coordinator restart | buckets reinitialize full → brief over-admission | ≤ burst B per tenant (accepted per spec) |
| worker restart | starts empty; leases on first request | first requests 429 until first grant lands (~ms) |
| tenant floods | isolated: per-tenant buckets/pools; hot tenant's rejects are cheap (local counter check) | no cross-tenant impact |

## 7. Deployment

- GKE cluster `cluster-1`, namespace `ratelimiter`.
- Image: single multi-stage Dockerfile → Artifact Registry (pushed via `gcloud`).
- Hand-applied bootstrap (checked into `deploy/`): namespace, ConfigMap, RBAC (ServiceAccount + Role + RoleBinding), loadgen Deployment + LoadBalancer Service. The loadgen creates/manages everything else.
- Left running for evaluators; README documents the external IP + `run.sh` usage.

## 8. Repo layout & build

```
cmd/ratelim/           # main; subcommands coordinator|worker|loadgen
internal/bucket/       # token bucket (unit-tested)
internal/coordinator/  # HTTP server, config load
internal/worker/       # local pools, lease client (unit-tested)
internal/loadgen/      # scenario engine, k8s ops, driver, report
deploy/                # bootstrap YAML
Dockerfile  Makefile   # build / test / push / deploy targets
run.sh                 # evaluator CLI wrapper around curl
```

Unit tests only where the logic is subtle (bucket refill math, worker pool/expiry/singleflight); the harness itself is the integration test.

**Time budget:** setup + skeleton 1h · coordinator + worker 2h · loadgen + scenarios 1.5h · deploy + GKE debugging + parameter tuning 1h · README/polish 0.5h. The tuning pass runs the scenarios with the hidden override parameters to settle lease size, watermark, tenant limits, and pass thresholds.

## 9. Design decisions

**Explicit:**
- D1 — Grants are debits; coordinator tracks no leases. Simplest possible coordinator; cost: expired unused tokens aren't reclaimed (small, bounded under-admission).
- D2 — Fail-closed on empty pool, per spec, and the request path never waits on the coordinator **when the tenant is known-exhausted** (zero-grant pacing or coordinator backoff active). Low-watermark prefetch keeps boundary 429s negligible: at the 20% watermark a worker still has ~2 tokens ≈ 40 ms of runway at 50 rps/worker, vastly more than the ~1–2 ms in-cluster lease round-trip. *Refined during tuning:* an empty pool with a renewal in flight and no known-exhausted signal (a cold or expired pool — e.g. a connection re-pinned to a worker that had gone idle for that tenant) waits up to 25 ms for the renewal instead of returning a spurious 429; in practice that is one ~2 ms round-trip. Flooding tenants and coordinator-down cases still fail fast.
- D3 — Single fixed lease size/duration for all tenants (per spec). Cost: high-limit tenants renew proportionally more often (renewals ≈ traffic/leaseSize). Defaults: size 10, duration 2s.
- D4 — One Go binary / one image, HTTP + JSON. Minimizes build and deploy surface within the 6h budget.
- D5 — Loadgen owns rate limiter lifecycle via the k8s API (per spec); only bootstrap YAML is applied by hand.
- D6 — Burst capacity = 1s of rate. Not in the spec's ConfigMap list; hard-coded multiplier rather than new config.
- D7 — Loadgen on a public LoadBalancer IP, guarded by a bearer token hard-coded in `run.sh` and the loadgen (checked into the repo). Demo-grade: keeps internet scanners out; RBAC scoped to the namespace and a fixed scenario set bound the blast radius regardless.
- D8 — Config read at startup only; change limits by editing the ConfigMap and restarting the coordinator.
- D9 — Every scenario self-judges: hard-coded assertions over the collected metrics produce a final `PASS`/`FAIL: <reasons>` line, and `run.sh` propagates it as its exit code. Thresholds start at the §5 values and get tuned during deployment.
- D10 — No evaluator-facing load parameters: per-tenant rate/concurrency are hard-coded per scenario. `run.sh` takes only the scenario name; hidden query overrides remain for our own tuning.

**Implicit assumptions:**
- "Rate limit" = requests/sec per tenant, globally, 1 token per request.
- Unknown tenants are rejected (404), not admitted.
- Latency is reported over all responses (admitted + rejected) per tenant.
- Coordinator kill mid-scenario resets its counters; the report notes this for the coordinator-kill scenario.
- Cluster as-is is sufficient (3 small nodes is plenty given workers are CPU-throttled deliberately).

## 10. Resolved questions (design review, 2026-08-16)

1. Boundary 429s: the prefetch watermark alone is sufficient (see D2 arithmetic); no waiting in the request path.
2. Loadgen access: bearer token hard-coded in `run.sh` and loadgen (D7).
3. Graphs: ASCII charts in the stream for now; HTML results page only if time remains.
4. Load parameters: hard-coded per scenario, not evaluator-settable (D10).
5. Tuning: lease duration fixed at 2 s. Lease size (initial 10), tenant limits (initial 100/50/20 rps), prefetch watermark (initial 20%), and pass-criteria thresholds are initial values to be validated by experiment on GKE — using the hidden override parameters — before being finalized.
6. New requirement folded in: each run ends with `PASS`/`FAIL` (D9), and the scenario description is printed at the start of each run and in `run.sh --help`.

## 11a. Findings from the GKE tuning pass (2026-08-16)

- **Lease size stays 10.** An experiment with size 20 made the smallest tenant (limit 20 rps) *worse*: one grant equals its entire 1-second burst budget, so a single worker drains the whole global bucket, starving peers into zero-grant pacing while the hoarded tokens partially expire. Empirical confirmation of the spec's "lease size must be a small fraction of the limit" guidance.
- **Per-scenario worker CPU caps.** With workers capped at 100m everywhere, the hot-tenant flood CPU-starved the workers and CFS throttling pushed *every* tenant's p99 to ~80 ms — worker overload (future work F2), not limiter behavior. Workers now run at a 400m limit except in the scaling scenario, which keeps 100m to make saturation reachable.
- **Requests ≠ limits on small nodes.** GKE system pods consume ~500m+ of each e2-medium node's 940m allocatable CPU, so pods request small (50–200m) and cap via limits (Burstable QoS); worker and loadgen deployments use `Recreate` so spec changes never need surge headroom.
- **Cold-start 429s → bounded wait** (see D2 refinement): under-limit tenants with few connections saw ~2% spurious rejects when connections re-pinned to workers whose pools had expired. Fixed by the 25 ms bounded wait; zero-grant poll floor also raised 100→200 ms to halve coordinator chatter from over-limit tenants. The wait applies only to genuinely cold/expired pools — an early version that waited on every racing exhaustion serialized high-rate tenants behind singleflight renewals and capped throughput (regression-tested now).
- **kube-proxy balances per connection, and programs iptables late.** The scaling scenario was flat across 1/2/4 replicas for three straight runs because the driver's keep-alive connections all pointed at the original pod — new replicas received literally zero requests (proven by per-worker counters, now printed in the output). Two fixes: drop idle connections between phases, and wait until every worker pod has *demonstrably served* Service traffic (probes with keep-alive disabled, so each probe re-rolls the DNAT choice) — pod readiness alone leads endpoint propagation by up to ~10 s on GKE. The same propagation lag dominates coordinator-kill recovery time (~4 s pod start + ~10 s propagation + ≤2 s lease backoff), so time-to-recover is reported as a measurement while the pass criterion asserts on the settled final 10 s.
- **The load generator is part of the system under test.** On a shared 2-vCPU node it generates ~2000 rps cleanly; beyond that, client-side queueing caps throughput at `concurrency / latency` regardless of server capacity (three identical flat-line runs before diagnosis). The scaling scenario is sized so 4 workers' total capacity (~2300/s at 50m each) stays inside the client's clean range.
- **Scenario runtime knobs** (final values): baseline/hot-tenant 30 s; scaling 3×25 s at 2500 rps offered, concurrency 200, workers 50m (25 s phases average out CFS/placement noise that swung 15 s phases by ~30%); worker-kill 45 s, kill at t=15; coordinator-kill 60 s, kill t=10, restore t=25.
- **Repeatability, measured (2026-08-16):** after the hardening above, the full five-scenario suite was run 5× back-to-back against the live deployment: **25/25 scenario runs passed**. Scaling ratios clustered at 2.21–2.43× (2 replicas, threshold ≥1.3×) and 3.75–3.92× (4 replicas, threshold ≥2.0×) — the 25 s phases cut the ratio spread by roughly 4× versus 15 s phases. Coordinator-kill recovery was 13–14 s after restore in every trial (pod start + endpoint propagation + lease backoff), consistent with the platform analysis above. Worker-kill turned out *cleaner* than designed — zero client-visible errors — because Go's HTTP client transparently retries idempotent requests whose reused connection died with the pod.

## 11. Future work

Not in scope for the initial 6-hour build; candidates for leftover time, roughly in the order I'd do them (each includes the scenario that would demonstrate it). Costs are rough.

**F1 — Graceful worker drain (S, ~30 min).** On SIGTERM, a worker POSTs its unused tokens back to a coordinator `/v1/return` endpoint (credit the tenant bucket, capped at capacity) before exiting. Scale-downs then strand nothing. Demo: rerun `worker-kill` with a graceful scale-down instead of force-delete and show zero stranded capacity / no admission dip. Also the only future item that touches the "grants are debits" model, and only additively.

**F2 — Worker overload protection with tenant-fair shedding (M, ~1–1.5 h).** Today a worker saturated with more aggregate traffic than it can serve degrades for everyone. Add a cheap load signal (in-flight request count against a per-worker cap, standing in for CPU) and shed *before* touching limiter state when over it, returning 503 + `Retry-After` (deliberately distinct from 429 = "over your limit"). Fairness: divide the in-flight cap into per-tenant shares (max-min style — equal shares, unused share redistributed), so a flooding tenant absorbs essentially all of the shedding. New scenario `worker-overload`: drive aggregate load past worker capacity with one flooding tenant; assert low-rate tenants keep near-100% admission and low p99 while the flooder eats the 503s.

**F3 — Adaptive lease size (M, ~1–1.5 h).** Fixed lease size means renewal rate scales linearly with a tenant's traffic (renewals ≈ rate/size) and cold tenants strand a full lease. Let each worker track an EWMA of per-tenant consumption and request a size hint (`target: one renewal per ~1 s of observed demand`); the coordinator clamps the hint to `[minSize, maxFraction × R × D]` and stays otherwise stateless — the adaptivity lives in the worker, preserving the grants-are-debits model. Demo: extend `baseline` to sweep a tenant's rate 10×→ and assert renewals/sec stays roughly flat instead of growing 10×. Lease *duration* could adapt similarly but has weaker payoff; size is where the win is.

**F4 — Peer-to-peer budget borrowing during coordinator outage (L, ~2 h).** Workers discover peers via a headless Service. When the coordinator is unreachable and a tenant's pool is empty, a worker asks peers to *transfer* some of their unused tokens for that tenant (transfer, not copy — the tokens were already debited globally, so the global invariant survives; forwarded with the original grant's remaining TTL so expiry still bounds staleness). The system then fails closed only when the *cluster-wide* outstanding budget is exhausted, not per-worker. Demo: extend `coordinator-kill` to compare admitted-during-outage with borrowing on vs. off; assert longer sustain and total outage admissions still ≤ total outstanding leased budget. The most interesting distributed-systems item, and the most subtle (peer discovery, request fan-out limits, avoiding borrow storms — e.g., only borrow when a peer reports > 1 lease-worth spare, with jittered polling).

**F5 — Sticky tenant→worker routing (M, ~1 h).** Spraying a tenant across all workers multiplies stranded capacity and renewal traffic by the worker count. Consistent-hash tenants to a small subset of workers (client-side in the loadgen, standing in for a smart proxy). Demo: assert renewal counts and admission-rate accuracy improve vs. random spraying, especially for low-limit tenants. Cheap to build; makes a nice "systems insight" talking point (effective lease size ∝ 1/workers-per-tenant).

**F6 — Weighted requests — ✅ implemented (2026-08-16).** Admission cost as a query param (`?cost=5`) debiting multiple tokens, demonstrating weighted rate limiting (e.g., expensive API calls). Folded into `baseline` with a mixed-cost workload; the pass criterion asserts admitted *token* rate (not request rate) tracks the limit. Admission is all-or-nothing, and an unsatisfiable cost triggers a renewal even above the watermark (starvation guard).

**F7 — Coordinator HA via leader election (L, ~2 h+).** Active-standby coordinators using the Kubernetes Lease API; standby takes over with empty buckets (bounded over-admission per D-crash semantics, or warm-synced via F1-style returns). Shrinks the fail-closed window from "until pod reschedules" to seconds. Demo: `coordinator-kill` variant asserting a much shorter 429 window. Biggest lift, and partially redundant with F4.

**F8 — Global-invariant assertion in every scenario — ✅ implemented (2026-08-16).** A universal check, appended to every scenario's criteria (including the failure ones and each scaling phase): per tenant, admitted tokens ≤ R × window + B, plus bounded slack for pre-leased worker pools and one extra burst per coordinator restart. Turns the design's core claim — and the crash over-admission bound from §6 — into a continuously verified property.

Deliberately omitted: Prometheus/Grafana observability and the HTML results page (nice demos, but they demonstrate plumbing rather than rate-limiter properties; the ASCII charts already carry the story), and config hot-reload (operationally nice, demonstrates nothing distributed).
