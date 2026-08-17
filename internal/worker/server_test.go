package worker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// checkVia drives one request through the worker's real HTTP handler.
func checkVia(t *testing.T, h http.Handler, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

func TestHandlerStatusMapping(t *testing.T) {
	fl := &fakeLeaser{grant: Grant{Tokens: 2, TTL: time.Minute}}
	h := (&Server{limiter: NewLimiter(fl), id: "w"}).Handler()

	if got := checkVia(t, h, "/v1/check/a"); got != http.StatusOK {
		t.Fatalf("admit = %d, want 200", got)
	}
	checkVia(t, h, "/v1/check/a") // drains the second token
	fl.mu.Lock()
	fl.grant = Grant{Tokens: 0, RetryAfter: time.Hour}
	fl.mu.Unlock()
	if got := checkVia(t, h, "/v1/check/a"); got != http.StatusTooManyRequests {
		t.Fatalf("reject = %d, want 429", got)
	}
}

func TestHandlerUnknownTenant404(t *testing.T) {
	h := (&Server{limiter: NewLimiter(&fakeLeaser{err: ErrUnknownTenant}), id: "w"}).Handler()
	if got := checkVia(t, h, "/v1/check/nosuch"); got != http.StatusNotFound {
		t.Fatalf("unknown = %d, want 404", got)
	}
}

func TestHandlerCostValidation(t *testing.T) {
	fl := &fakeLeaser{grant: Grant{Tokens: 10, TTL: time.Minute}}
	h := (&Server{limiter: NewLimiter(fl), id: "w"}).Handler()

	for _, bad := range []string{"abc", "0", "-1", "1001", "1.5"} {
		if got := checkVia(t, h, "/v1/check/a?cost="+bad); got != http.StatusBadRequest {
			t.Fatalf("cost=%s = %d, want 400", bad, got)
		}
	}
	if fl.callCount() != 0 {
		t.Fatal("invalid cost must be rejected before touching the limiter")
	}
	if got := checkVia(t, h, "/v1/check/a?cost=5"); got != http.StatusOK {
		t.Fatalf("cost=5 = %d, want 200", got)
	}
	if got := lmCounters(h, t); got != 5 {
		t.Fatalf("admitted tokens = %d, want 5 (cost honored)", got)
	}
}

// lmCounters digs the admitted-token count for tenant "a" out of the handler's
// stats endpoint, keeping the test on the public surface.
func lmCounters(h http.Handler, t *testing.T) int64 {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/stats", nil))
	var body struct {
		Tenants map[string]map[string]int64 `json:"tenants"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body.Tenants["a"]["admittedTokens"]
}
