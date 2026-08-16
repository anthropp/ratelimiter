# Distributed Rate Limiter

A scalable, fault-tolerant distributed rate limiter that degrades gracefully under load
and failures, deployed on GKE with a self-judging evaluation harness. Built as a
take-home project; the full design (with explicit trade-offs and tuning findings) is in
[DESIGN.md](DESIGN.md), and the original brief is in `dist-ratelim.txt`.

## Try it (no credentials needed)

The system is running on GKE. All you need is `curl` (and `python3` for `--help`
formatting):

```
./run.sh --help              # list scenarios with descriptions
./run.sh baseline            # enforcement at the limit, across concurrent workers
./run.sh hot-tenant          # tenant isolation under a 20x flood
./run.sh scaling             # decision throughput at 1, 2, 4 worker replicas
./run.sh worker-kill         # abrupt worker death, no replacement
./run.sh coordinator-kill    # fail-closed while the coordinator is down, then recovery
```

Each run streams live progress, ends with a per-tenant report (sent / admitted /
rejected / errors by code / p50 / p99, plus coordinator lease-call counts), renders
ASCII time-series charts for the failure scenarios, and prints a final `PASS` or
`FAIL: <reasons>` verdict from hard-coded assertions. `run.sh` exits 0 on PASS, 1 on
FAIL. One scenario runs at a time (HTTP 409 otherwise).

## Architecture

```
 evaluator ── curl ──► loadgen (LoadBalancer) ── admission traffic ──► worker Service
                         │                                               worker × N
                         │ k8s API: create / scale / kill                  │ lease
                         ▼                                                 ▼
                       coordinator + worker Deployments              coordinator (1)
                                                                   ConfigMap: lease size,
                                                                   duration, tenant limits
```

- **Coordinator** holds one token bucket per tenant (refill = configured limit, burst =
  1 s). A lease request debits up to `lease.size` tokens and returns the grant —
  *grants are debits*: the coordinator tracks no leases, only buckets and counters, so
  the global invariant (admitted ≤ rate × window + burst) holds for any worker count.
- **Workers** admit from local leased tokens; the hot path never blocks on the
  coordinator (one bounded exception: a genuinely cold/expired pool may wait ≤25 ms for
  the in-flight lease instead of returning a spurious 429). Leases are fetched on
  demand with singleflight, prefetched at a 20% low-watermark, and expire after
  `lease.durationMs`. If the coordinator is down, workers drain their budget and then
  **fail closed** (clean 429s), retrying with backoff until it returns.
- **Loadgen** owns the rate limiter's lifecycle through the Kubernetes API (creating,
  scaling, and force-killing pods per scenario), drives paced per-tenant load, and
  judges the outcome. It is the only hand-deployed component.

All three roles are one Go binary (`ratelim`) in one container image, plain HTTP+JSON.
All state is in-memory except the ConfigMap, by design: crash-restart may briefly
over-admit (bounded by burst), which the design accepts in exchange for a trivially
simple coordinator.

## Repo tour

```
cmd/ratelim/            entrypoint: coordinator | worker | loadgen
internal/bucket/        token bucket (unit-tested, incl. the global invariant)
internal/config/        ConfigMap YAML parsing
internal/coordinator/   /v1/lease, /v1/stats
internal/worker/        local pools + lease client (unit-tested state machine)
internal/loadgen/       k8s lifecycle, load driver, scenarios, report, HTTP server
deploy/                 bootstrap YAML: namespace, ConfigMap, RBAC, loadgen
run.sh                  evaluator CLI (bearer token baked in — see below)
```

## Operating it

```
make test        # unit tests
make image       # build+push the image with Cloud Build (no local Docker needed)
make bootstrap   # kubectl apply -f deploy/   (one-time)
make redeploy    # rebuild image and restart the running components
make url         # print the loadgen's external URL
```

Tenant limits and lease parameters live in `deploy/configmap.yaml`
(tenant-a/b/c = 100/50/20 rps, tenant-hi = 1000 rps for the scaling scenario; lease =
10 tokens / 2 s). Apply changes with `kubectl apply -f deploy/configmap.yaml` and
restart the coordinator.

**A note on auth:** the control endpoint is public; `/run` requires a bearer token that
is deliberately hard-coded in `run.sh` and checked into this repo. It is demo-grade
auth — its only job is keeping internet scanners from triggering pod kills. RBAC scopes
the loadgen to its namespace, and only the fixed scenario set is exposed.

## What the scenarios demonstrate

| scenario | property | headline result |
|---|---|---|
| baseline | enforcement at the limit across concurrent workers; leases ≪ requests | admitted ≈ limit ±1%, ~1 lease call per 10 requests |
| hot-tenant | isolation: a 20× flood cannot hurt other tenants | quiet tenants 100% admitted, p99 ≈ 3 ms; flooder pinned at its limit |
| scaling | decision throughput grows with replicas | ~660 → ~1480 → ~2340 decisions/s at 1→2→4 |
| worker-kill | abrupt worker death, no replacement | zero client-visible errors; steady state on the survivor |
| coordinator-kill | fail-closed, then recovery | 0 admitted + clean 429s during outage, 0 errors; recovers ≈8–15 s after restore |

DESIGN.md §11a records what deployment tuning surfaced — per-connection kube-proxy
balancing, ~10 s endpoint propagation, CFS throttling, and why lease size stayed at 10
— and §11 lists future work (tenant-fair overload shedding, adaptive lease sizing,
peer-to-peer budget borrowing during coordinator outages, and more).
