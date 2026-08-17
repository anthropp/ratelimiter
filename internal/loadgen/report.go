package loadgen

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/anthropp/ratelimiter/internal/coordinator"
)

// Check is one pass criterion evaluated at the end of a scenario.
type Check struct {
	Name   string
	OK     bool
	Detail string
}

func check(name string, ok bool, detailFmt string, args ...any) Check {
	return Check{Name: name, OK: ok, Detail: fmt.Sprintf(detailFmt, args...)}
}

func renderChecks(w *Stream, checks []Check) bool {
	w.Printf("\nCHECKS\n")
	pass := true
	var failed []string
	for _, c := range checks {
		mark := "PASS"
		if !c.OK {
			mark = "FAIL"
			pass = false
			failed = append(failed, c.Name)
		}
		w.Printf("  [%s] %s: %s\n", mark, c.Name, c.Detail)
	}
	w.Printf("\n")
	if pass {
		w.Printf("PASS\n")
	} else {
		w.Printf("FAIL: %s\n", strings.Join(failed, "; "))
	}
	return pass
}

// renderTable prints the per-tenant summary table the spec asks for.
func renderTable(w *Stream, rs Results) {
	w.Printf("\n%-11s %9s %9s %9s %9s %7s %9s %9s   %s\n",
		"TENANT", "SENT", "ADMITTED", "ADM.TOK", "REJECTED", "DROPS", "p50(ms)", "p99(ms)", "ERRORS")
	for _, name := range rs.TenantsSorted() {
		r := rs[name]
		r.mu.Lock()
		sent, adm, admTok, rej, drops := r.Sent, r.Admitted, r.AdmittedTokens, r.Rejected, r.ClientDrops
		errs := make([]string, 0, len(r.Errors))
		for code, n := range r.Errors {
			errs = append(errs, fmt.Sprintf("%s:%d", code, n))
		}
		r.mu.Unlock()
		sort.Strings(errs)
		errStr := "-"
		if len(errs) > 0 {
			errStr = strings.Join(errs, " ")
		}
		w.Printf("%-11s %9d %9d %9d %9d %7d %9.1f %9.1f   %s\n",
			name, sent, adm, admTok, rej, drops, r.Percentile(50), r.Percentile(99), errStr)
	}
}

// renderChart draws a per-second ASCII time series, one column per second
// (two seconds per column if the run is long).
func renderChart(w *Stream, title string, series []int) {
	if len(series) == 0 {
		return
	}
	colSec := 1
	if len(series) > 70 {
		colSec = 2
	}
	var cols []int
	for i := 0; i < len(series); i += colSec {
		v := series[i]
		if colSec == 2 && i+1 < len(series) {
			v = (v + series[i+1]) / 2
		}
		cols = append(cols, v)
	}
	max := 1
	for _, v := range cols {
		if v > max {
			max = v
		}
	}
	const height = 8
	w.Printf("\n%s (peak %d/s)\n", title, max)
	for row := height; row >= 1; row-- {
		hi := float64(max) * float64(row) / height
		lo := float64(max) * (float64(row) - 0.5) / height
		line := make([]byte, len(cols))
		for i, v := range cols {
			switch {
			case float64(v) >= hi:
				line[i] = '#'
			case float64(v) >= lo:
				line[i] = '.'
			default:
				line[i] = ' '
			}
		}
		label := ""
		if row == height {
			label = fmt.Sprintf("%d", max)
		}
		w.Printf("%6s |%s\n", label, string(line))
	}
	w.Printf("%6s +%s\n", "0", strings.Repeat("-", len(cols)))
	// X-axis: mark every 10 seconds.
	axis := make([]byte, len(cols))
	for i := range axis {
		axis[i] = ' '
	}
	for s := 0; s*colSec/10 <= len(cols)*colSec/10; s += 10 {
		i := s / colSec
		if i < len(axis) {
			axis[i] = '|'
		}
	}
	w.Printf("%6s  %s\n%6s  (x: seconds, %ds/col)\n", "", string(axis), "", colSec)
}

// sumSeries merges per-tenant buckets into one per-second series.
func sumSeries(rs Results, metric func(SecBucket) int) []int {
	maxLen := 0
	for _, r := range rs {
		r.mu.Lock()
		if len(r.Buckets) > maxLen {
			maxLen = len(r.Buckets)
		}
		r.mu.Unlock()
	}
	out := make([]int, maxLen)
	for _, r := range rs {
		r.mu.Lock()
		for i, b := range r.Buckets {
			out[i] += metric(b)
		}
		r.mu.Unlock()
	}
	return out
}

// CoordStats scrapes the coordinator's counters; used for renewal-count deltas.
func CoordStats(coordURL string) (*coordinator.Stats, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(coordURL + "/v1/stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("stats returned %d", resp.StatusCode)
	}
	var s coordinator.Stats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func totalLeaseRequests(s *coordinator.Stats) int64 {
	if s == nil {
		return 0
	}
	var n int64
	for _, t := range s.Tenants {
		n += t.LeaseRequests
	}
	return n
}

func totalSent(rs Results) int {
	n := 0
	for _, r := range rs {
		r.mu.Lock()
		n += r.Sent
		r.mu.Unlock()
	}
	return n
}
