package coordinator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anthropp/ratelimiter/internal/config"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	cfg := &config.Config{Tenants: map[string]float64{"t1": 10}} // burst cap 10
	cfg.Lease.Size = 10
	cfg.Lease.DurationMs = 2000
	srv := httptest.NewServer(New(cfg).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func lease(t *testing.T, srv *httptest.Server, body string) (int, LeaseResponse) {
	t.Helper()
	resp, err := srv.Client().Post(srv.URL+"/v1/lease", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var lr LeaseResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode, lr
}

func TestLeaseGrantThenZeroGrantWithRetryAfter(t *testing.T) {
	srv := newTestServer(t)

	code, lr := lease(t, srv, `{"tenant":"t1","worker":"w"}`)
	if code != http.StatusOK || lr.Granted != 10 || lr.TTLMs != 2000 {
		t.Fatalf("first lease = %d %+v, want 200 granted=10 ttlMs=2000", code, lr)
	}
	// Bucket drained: an immediate second lease must be a zero grant with a
	// pacing hint, not an error.
	code, lr = lease(t, srv, `{"tenant":"t1","worker":"w"}`)
	if code != http.StatusOK || lr.Granted != 0 || lr.RetryAfterMs < 1 {
		t.Fatalf("drained lease = %d %+v, want 200 granted=0 retryAfterMs>=1", code, lr)
	}
}

func TestLeaseUnknownTenant404(t *testing.T) {
	srv := newTestServer(t)
	if code, _ := lease(t, srv, `{"tenant":"nosuch","worker":"w"}`); code != http.StatusNotFound {
		t.Fatalf("unknown tenant = %d, want 404", code)
	}
}

func TestLeaseBadRequest400(t *testing.T) {
	srv := newTestServer(t)
	for _, body := range []string{`not json`, `{}`, `{"worker":"w"}`} {
		if code, _ := lease(t, srv, body); code != http.StatusBadRequest {
			t.Fatalf("body %q = %d, want 400", body, code)
		}
	}
}

func TestLeaseRequiresPost(t *testing.T) {
	srv := newTestServer(t)
	resp, err := srv.Client().Get(srv.URL + "/v1/lease")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /v1/lease = %d, want 405", resp.StatusCode)
	}
}

func TestStatsCountersReflectLeases(t *testing.T) {
	srv := newTestServer(t)
	lease(t, srv, `{"tenant":"t1","worker":"w"}`) // granted 10
	lease(t, srv, `{"tenant":"t1","worker":"w"}`) // zero grant

	resp, err := srv.Client().Get(srv.URL + "/v1/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s Stats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	ts := s.Tenants["t1"]
	if ts.LeaseRequests != 2 || ts.ZeroGrants != 1 || ts.TokensGranted != 10 {
		t.Fatalf("stats = %+v, want leaseRequests=2 zeroGrants=1 tokensGranted=10", ts)
	}
	if s.StartedMs == 0 {
		t.Fatal("startedMs missing; the harness uses it to detect restarts")
	}
}
