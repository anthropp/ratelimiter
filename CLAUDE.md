# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A distributed rate limiter built as a take-home assignment for a job application (not for production). The full requirements and design brief live in `dist-ratelim.txt` — **read it before making design or implementation decisions**. Total budget for design, implementation, deployment, and testing is six hours, so favor simplicity.

The repository is currently greenfield (no code yet). The `.gitignore` is the standard Go template, so Go is the intended implementation language. As build/test/deploy commands are established, record them here.

## Architecture (from the spec)

Three components, all running on GKE:

1. **Coordinator** (single instance) — holds global per-tenant rate limits and grants leases of quota to workers. Configured via a Kubernetes ConfigMap (lease size, lease duration, per-tenant limits). Uses one fixed lease size/duration for every `<tenant, worker>` pair.
2. **Worker rate limiter replicas** (horizontally scalable) — make admission decisions locally against leased quota. They lease budget on demand (first request for a tenant, or when local budget is exhausted) and renew periodically. If the coordinator is down, they keep admitting until their leased budget runs out, then **fail closed** (reject) for that tenant.
3. **Load generator / evaluation harness** (single pod) — replaces the app-server tier. Takes per-tenant rate, concurrency, and a hard-coded scenario name as CLI args. It also drives the Kubernetes API itself (creating rate limiter components, killing pods for failure scenarios). Must be invocable remotely from an evaluator's laptop without authentication and without kubectl.

Key design constraints:
- All state is in-memory except the ConfigMap. Losing state on crash (and the resulting over-admission) is acceptable.
- Lease size must be a small fraction of the global limit so crashed replicas don't strand much capacity and new replicas can still get quota.
- High load from one tenant must not degrade other tenants.
- The harness reports per-tenant: requests sent/admitted/rejected, errors by HTTP code, p50/p99 latency, and ideally coordinator lease-renewal counts (to prove workers aren't consulting the coordinator per request). Failure scenarios should graph a metric over time.

Required evaluation scenarios: normal operation (429s above configured rate, across concurrent workers), hot tenant isolation, throughput scaling with 1/2/4 worker replicas (workers get artificially low CPU limits so one load generator can saturate them), worker pod kill, and coordinator kill (workers keep admitting then fail closed).

## Environment

- Deployment target: GKE cluster `cluster-1`, accessible via `kubectl`. The `gcloud` and `gh` CLIs are installed and authenticated. Cluster node count/VM type may be changed if it helps the demonstration scenarios.
- Deploy from the current git branch; check all code into GitHub. The deployed system is left running for evaluators to try.
