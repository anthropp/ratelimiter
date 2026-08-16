// Package coordinator serves the global per-tenant token buckets.
//
// The coordinator tracks no leases: a grant simply debits the tenant's global
// bucket ("grants are debits", design D1). Its entire state is the buckets
// plus monotonic counters used by the evaluation harness.
package coordinator

import (
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/anthropp/ratelimiter/internal/bucket"
	"github.com/anthropp/ratelimiter/internal/config"
)

type tenantState struct {
	bucket        *bucket.Bucket
	leaseRequests atomic.Int64 // every /v1/lease call for this tenant
	zeroGrants    atomic.Int64 // lease calls answered with granted=0
	tokensGranted atomic.Int64
}

type Server struct {
	cfg     *config.Config
	tenants map[string]*tenantState
	started time.Time
}

func New(cfg *config.Config) *Server {
	s := &Server{cfg: cfg, tenants: make(map[string]*tenantState), started: time.Now()}
	now := time.Now()
	for name, rate := range cfg.Tenants {
		s.tenants[name] = &tenantState{bucket: bucket.New(rate, rate*config.BurstSeconds, now)}
	}
	return s
}

type LeaseRequest struct {
	Tenant string `json:"tenant"`
	Worker string `json:"worker"`
}

type LeaseResponse struct {
	Granted      int   `json:"granted"`
	TTLMs        int64 `json:"ttlMs"`
	RetryAfterMs int64 `json:"retryAfterMs,omitempty"`
}

type TenantStats struct {
	LeaseRequests int64 `json:"leaseRequests"`
	ZeroGrants    int64 `json:"zeroGrants"`
	TokensGranted int64 `json:"tokensGranted"`
}

type Stats struct {
	StartedMs int64                  `json:"startedMs"` // unix ms; lets the harness detect restarts
	Tenants   map[string]TenantStats `json:"tenants"`
}

func (s *Server) handleLease(w http.ResponseWriter, r *http.Request) {
	var req LeaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Tenant == "" {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return
	}
	ts, ok := s.tenants[req.Tenant]
	if !ok {
		http.Error(w, `{"error":"unknown tenant"}`, http.StatusNotFound)
		return
	}
	ts.leaseRequests.Add(1)
	now := time.Now()
	granted := ts.bucket.TakeUpTo(s.cfg.Lease.Size, now)
	resp := LeaseResponse{Granted: granted, TTLMs: s.cfg.Lease.DurationMs}
	if granted == 0 {
		ts.zeroGrants.Add(1)
		resp.RetryAfterMs = ts.bucket.NextTokenIn(now).Milliseconds() + 1
	} else {
		ts.tokensGranted.Add(int64(granted))
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	out := Stats{StartedMs: s.started.UnixMilli(), Tenants: make(map[string]TenantStats, len(s.tenants))}
	for name, ts := range s.tenants {
		out.Tenants[name] = TenantStats{
			LeaseRequests: ts.leaseRequests.Load(),
			ZeroGrants:    ts.zeroGrants.Load(),
			TokensGranted: ts.tokensGranted.Load(),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/lease", s.handleLease)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return mux
}

func Run(configPath, addr string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	s := New(cfg)
	log.Printf("coordinator listening on %s; lease size=%d durationMs=%d tenants=%v",
		addr, cfg.Lease.Size, cfg.Lease.DurationMs, cfg.Tenants)
	return http.ListenAndServe(addr, s.Handler())
}
