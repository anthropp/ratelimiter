package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// AuthToken is deliberately hard-coded and checked into the repo (design D7):
// it is demo-grade auth whose only job is keeping internet scanners away from
// an endpoint that can kill pods. RBAC confines the blast radius regardless.
const AuthToken = "rldemo-c7f31a92d4e85b06"

// Stream is a line-oriented chunked-response writer; every Printf is flushed
// so the evaluator watches the run live.
type Stream struct {
	mu sync.Mutex
	w  http.ResponseWriter
	f  http.Flusher
}

func (s *Stream) Printf(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(s.w, format, args...)
	if s.f != nil {
		s.f.Flush()
	}
}

type server struct {
	kube     *Kube
	driver   *Driver
	coordURL string
	runMu    sync.Mutex // one scenario at a time
}

func (s *server) authorized(r *http.Request) bool {
	if r.Header.Get("Authorization") == "Bearer "+AuthToken {
		return true
	}
	return r.URL.Query().Get("token") == AuthToken // browser convenience
}

func (s *server) handleScenarios(w http.ResponseWriter, _ *http.Request) {
	type item struct{ Name, Description string }
	out := make([]item, 0, len(Scenarios))
	for _, sc := range Scenarios {
		out = append(out, item{sc.Name, sc.Description})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (s *server) handleRun(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		http.Error(w, "missing or wrong bearer token", http.StatusUnauthorized)
		return
	}
	sc := FindScenario(r.URL.Query().Get("scenario"))
	if sc == nil {
		names := ""
		for i, s := range Scenarios {
			if i > 0 {
				names += ", "
			}
			names += s.Name
		}
		http.Error(w, "unknown scenario; one of: "+names, http.StatusBadRequest)
		return
	}
	if !s.runMu.TryLock() {
		http.Error(w, "another scenario is already running; try again shortly", http.StatusConflict)
		return
	}
	defer s.runMu.Unlock()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, _ := w.(http.Flusher)
	out := &Stream{w: w, f: flusher}

	out.Printf("=== scenario: %s ===\n\n%s\n\n", sc.Name, sc.Description)

	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Minute)
	defer cancel()

	cfg, err := s.kube.LoadConfig(ctx)
	if err != nil {
		out.Printf("ERROR: reading %s ConfigMap: %v\nFAIL: setup error\n", configMapName, err)
		return
	}
	env := &Env{
		Kube:      s.kube,
		Driver:    s.driver,
		Out:       out,
		Cfg:       cfg,
		CoordURL:  s.coordURL,
		Overrides: r.URL.Query(),
	}
	start := time.Now()
	checks, err := sc.Run(ctx, env)
	if err != nil {
		out.Printf("\nERROR: %v\nFAIL: scenario error\n", err)
		return
	}
	pass := renderChecks(out, checks)
	log.Printf("scenario %s finished in %v pass=%v", sc.Name, time.Since(start).Round(time.Second), pass)
}

// Run starts the loadgen control server.
func Run(addr, workerURL, coordURL string) error {
	kube, err := NewKube()
	if err != nil {
		return err
	}
	s := &server{kube: kube, driver: NewDriver(workerURL), coordURL: coordURL}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /scenarios", s.handleScenarios)
	mux.HandleFunc("GET /run", s.handleRun)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	log.Printf("loadgen listening on %s (namespace %s, image %s)", addr, kube.ns, kube.image)
	srv := &http.Server{Addr: addr, Handler: mux, WriteTimeout: 15 * time.Minute}
	return srv.ListenAndServe()
}
