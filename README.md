# Distributed Rate Limiter

A scalable, fault-tolerant distributed rate limiter that degrades gracefully under load
and failures, deployed on GKE with a self-judging evaluation harness. Built as a
take-home project; the full design (with explicit trade-offs and tuning findings) is in
[DESIGN.md](DESIGN.md), and the original brief is in [dist-ratelim.txt](dist-ratelim.txt).

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
FAIL. One scenario runs at a time; if another is already in progress, `run.sh` waits
and retries automatically.

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

- **Coordinator** holds one token bucket per tenant, refilling at the configured limit
  with burst capacity equal to one second of that rate. A lease request debits up to
  `lease.size` tokens and returns the grant — *grants are debits*: the coordinator
  tracks no leases, only buckets and counters, so the core guarantee (tokens admitted
  over any window never exceed rate × window + burst) holds no matter how many workers
  there are.
- **Workers** make each admission decision from their local pool of leased tokens
  (a request may declare a token cost via `?cost=N` — weighted rate limiting, admitted
  or rejected whole, never partially). Handling a request never waits on the
  coordinator, with one bounded exception: a pool that is empty because it was never
  filled (or its lease expired) may wait up to 25 ms for the lease already being
  fetched, instead of returning a needless 429. Leases are fetched on demand —
  concurrent requests share one in-flight lease call — and renewed in the background
  once the pool drops below 20% of the last grant; each grant expires after
  `lease.durationMs`. If the coordinator is down, workers spend what they have and then
  **fail closed** (clean 429s), retrying with increasing delays until it returns.
- **Loadgen** owns the rate limiter's lifecycle through the Kubernetes API (creating,
  scaling, and force-killing pods per scenario), drives paced per-tenant load, and
  judges the outcome. It is the only hand-deployed component. Every scenario also
  asserts the guarantee above per tenant, allowing only a small, bounded margin for
  tokens that were already leased out to workers when the measurement began.

All three roles are one Go binary (`ratelim`) in one container image, plain HTTP+JSON.
All state is in-memory except the ConfigMap, by design: after a crash and restart the
system may briefly admit more than the limit (by at most one burst), which the design
accepts in exchange for a trivially simple coordinator.

## Repo tour

```
cmd/ratelim/            entrypoint: coordinator | worker | loadgen
internal/bucket/        token bucket (unit-tested, incl. the core guarantee)
internal/config/        ConfigMap YAML parsing + validation (tested)
internal/coordinator/   /v1/lease, /v1/stats (handler-level tests)
internal/worker/        local pools + lease client (tested state machine, plus
                        integration tests that run a real coordinator in-process
                        to verify the HTTP protocol between the two)
internal/loadgen/       k8s lifecycle, load driver, scenarios, report, HTTP server
                        (tests for the guarantee arithmetic and the PASS/FAIL
                        output format run.sh depends on)
deploy/                 bootstrap YAML: namespace, ConfigMap, RBAC, loadgen
run.sh                  evaluator CLI (bearer token baked in — see below)
dist-ratelim.txt        the original prompt given to Claude at the start of this project
DESIGN.md               the design doc Claude created from that prompt and our discussion
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
auth — its only job is keeping internet scanners from triggering pod kills. The
loadgen's Kubernetes permissions (RBAC) are limited to its own namespace, and only the
fixed scenario set is exposed.

## What the scenarios demonstrate

| scenario | property | headline result |
|---|---|---|
| baseline | enforcement at the limit across concurrent workers, with a mixed-cost weighted workload (`?cost=N`); far fewer coordinator calls than requests | admitted *token* rate ≈ limit ±1%; ~1 lease call per 5 requests (each request averages ~2 tokens) |
| hot-tenant | isolation: a 20× flood cannot hurt other tenants | quiet tenants 100% admitted, single-digit-ms p99; flooder pinned at its limit |
| scaling | decision throughput grows with replicas | ~620 → ~1200–1400 → ~2050–2350 decisions/s at 1→2→4 (ratios ≈ 2.0–2.4× and 3.3–3.9× across runs) |
| worker-kill | abrupt worker death, no replacement | zero client-visible errors; steady state on the survivor |
| coordinator-kill | fail-closed, then recovery | 0 admitted + clean 429s during outage, 0 errors; recovers ≈8–15 s after restore |

DESIGN.md §11a records what deployment tuning surfaced — Kubernetes spreading load per
connection rather than per request, ~10 s delays before new pods start receiving
Service traffic, Linux CPU-quota throttling skewing latency measurements, and why lease
size stayed at 10 — and §11 lists future work. Two of those items have since been
implemented: weighted requests (F6, the `?cost=N` workload above) and the
every-scenario guarantee assertion (F8). Still open: shedding load fairly across
tenants when a worker is overloaded, adapting lease size to each tenant's traffic, and
letting workers borrow unused budget from each other while the coordinator is down.
