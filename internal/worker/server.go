package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

// httpLeaser talks to the coordinator's /v1/lease.
type httpLeaser struct {
	url    string // e.g. http://coordinator:8081
	worker string
	client *http.Client
}

func (h *httpLeaser) Lease(ctx context.Context, tenant string) (Grant, error) {
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	body, _ := json.Marshal(map[string]string{"tenant": tenant, "worker": h.worker})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url+"/v1/lease", bytes.NewReader(body))
	if err != nil {
		return Grant{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		return Grant{}, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return Grant{}, ErrUnknownTenant
	default:
		return Grant{}, fmt.Errorf("coordinator returned %d", resp.StatusCode)
	}
	var lr struct {
		Granted      int   `json:"granted"`
		TTLMs        int64 `json:"ttlMs"`
		RetryAfterMs int64 `json:"retryAfterMs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		return Grant{}, err
	}
	return Grant{
		Tokens:     lr.Granted,
		TTL:        time.Duration(lr.TTLMs) * time.Millisecond,
		RetryAfter: time.Duration(lr.RetryAfterMs) * time.Millisecond,
	}, nil
}

type Server struct {
	limiter *Limiter
	id      string
}

func (s *Server) handleCheck(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")
	cost := 1
	if cs := r.URL.Query().Get("cost"); cs != "" {
		v, err := strconv.Atoi(cs)
		if err != nil || v < 1 || v > 1000 {
			http.Error(w, "cost must be an integer in [1,1000]", http.StatusBadRequest)
			return
		}
		cost = v
	}
	switch s.limiter.Check(tenant, cost) {
	case Admit:
		w.WriteHeader(http.StatusOK)
	case Reject:
		w.WriteHeader(http.StatusTooManyRequests)
	case Unknown:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"worker": s.id, "tenants": s.limiter.StatsSnapshot()})
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/check/{tenant}", s.handleCheck)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return mux
}

func Run(coordinatorURL, addr string) error {
	id, _ := os.Hostname()
	leaser := &httpLeaser{
		url:    coordinatorURL,
		worker: id,
		client: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}},
	}
	s := &Server{limiter: NewLimiter(leaser), id: id}
	log.Printf("worker %s listening on %s, coordinator %s", id, addr, coordinatorURL)
	return http.ListenAndServe(addr, s.Handler())
}
