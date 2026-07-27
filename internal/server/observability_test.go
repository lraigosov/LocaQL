package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func getJSON(t *testing.T, s *Server, path string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	var out map[string]any
	if res.Body.Len() > 0 {
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			t.Fatalf("decode %s response: %v", path, err)
		}
	}
	return res.Code, out
}

func TestMetricsEndpointReportsRequestCounts(t *testing.T) {
	s := newTestServer()

	getJSON(t, s, "/_emulator/health")
	getJSON(t, s, "/_emulator/health")
	notFoundReq := httptest.NewRequest(http.MethodGet, "/_emulator/does-not-exist", nil)
	notFoundRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(notFoundRes, notFoundReq)
	if notFoundRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for an unmatched emulator path, got %d", notFoundRes.Code)
	}

	code, metrics := getJSON(t, s, "/_emulator/metrics")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	requests, ok := metrics["requests"].(map[string]any)
	if !ok {
		t.Fatalf("expected requests object, got %v", metrics["requests"])
	}
	total, ok := requests["total"].(float64)
	if !ok || total < 3 {
		t.Fatalf("expected total >= 3 (the 3 requests above, plus this one not yet counted), got %v", requests["total"])
	}
	byStatusClass, ok := requests["byStatusClass"].(map[string]any)
	if !ok {
		t.Fatalf("expected byStatusClass object, got %v", requests["byStatusClass"])
	}
	if v, _ := byStatusClass["2xx"].(float64); v < 2 {
		t.Fatalf("expected at least 2 successful requests recorded, got %v", byStatusClass["2xx"])
	}
	if v, _ := byStatusClass["4xx"].(float64); v < 1 {
		t.Fatalf("expected at least 1 4xx request recorded (the unmatched path), got %v", byStatusClass["4xx"])
	}
	byRouteGroup, ok := requests["byRouteGroup"].(map[string]any)
	if !ok {
		t.Fatalf("expected byRouteGroup object, got %v", requests["byRouteGroup"])
	}
	if v, _ := byRouteGroup["emulator"].(float64); v < 3 {
		t.Fatalf("expected at least 3 requests classified under the emulator route group, got %v", byRouteGroup["emulator"])
	}
}

func TestMetricsLatencyBucketsSumToRequestTotal(t *testing.T) {
	s := newTestServer()
	for i := 0; i < 5; i++ {
		getJSON(t, s, "/_emulator/health")
	}
	_, metrics := getJSON(t, s, "/_emulator/metrics")
	requests := metrics["requests"].(map[string]any)
	total := requests["total"].(float64)
	buckets, ok := requests["latencyMsBuckets"].([]any)
	if !ok || len(buckets) == 0 {
		t.Fatalf("expected a non-empty latencyMsBuckets array, got %v", requests["latencyMsBuckets"])
	}
	var sum float64
	for _, b := range buckets {
		sum += b.(map[string]any)["count"].(float64)
	}
	if sum != total {
		t.Fatalf("expected latency bucket counts (%v) to sum to the request total (%v)", sum, total)
	}
}

func TestHealthAndReadinessIncludeSubsystemDiagnostics(t *testing.T) {
	s := newTestServer()
	for _, path := range []string{"/_emulator/health", "/_emulator/readiness"} {
		code, out := getJSON(t, s, path)
		if code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d", path, code)
		}
		subsystems, ok := out["subsystems"].(map[string]any)
		if !ok {
			t.Fatalf("%s: expected a subsystems object, got %v", path, out["subsystems"])
		}
		jobs, ok := subsystems["jobs"].(map[string]any)
		if !ok {
			t.Fatalf("%s: expected subsystems.jobs, got %v", path, subsystems["jobs"])
		}
		runQueue, ok := jobs["runQueue"].(map[string]any)
		if !ok {
			t.Fatalf("%s: expected jobs.runQueue, got %v", path, jobs["runQueue"])
		}
		if unbounded, _ := runQueue["unbounded"].(bool); !unbounded {
			t.Fatalf("%s: expected runQueue.unbounded=true with LOCAQL_JOB_WORKERS unset, got %v", path, runQueue)
		}
		sessions, ok := subsystems["sessions"].(map[string]any)
		if !ok {
			t.Fatalf("%s: expected subsystems.sessions, got %v", path, subsystems["sessions"])
		}
		if _, ok := sessions["active"]; !ok {
			t.Fatalf("%s: expected sessions.active, got %v", path, sessions)
		}
	}
}

func TestMetricsReflectsActiveSessionCount(t *testing.T) {
	s := newTestServer()
	runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT 1","createSession":true}`)

	_, metrics := getJSON(t, s, "/_emulator/metrics")
	sessions, ok := metrics["sessions"].(map[string]any)
	if !ok {
		t.Fatalf("expected sessions object, got %v", metrics["sessions"])
	}
	if active, _ := sessions["active"].(float64); active < 1 {
		t.Fatalf("expected at least 1 active session, got %v", sessions["active"])
	}
}

func TestJobsMetricsDistinguishesCompletedAndFailed(t *testing.T) {
	s := newTestServer()

	_, before := getJSON(t, s, "/_emulator/metrics")
	beforeJobs := before["jobs"].(map[string]any)
	completedBefore := beforeJobs["completedTotal"].(float64)
	failedBefore := beforeJobs["failedTotal"].(float64)

	runJobAndFetch(t, s, `{"configuration":{"query":{"query":"SELECT 1 AS one"}}}`)
	runJobAndFetch(t, s, `{"configuration":{"query":{"query":"SELECT FORCE_ERROR"}}}`)

	_, after := getJSON(t, s, "/_emulator/metrics")
	afterJobs := after["jobs"].(map[string]any)
	completedAfter := afterJobs["completedTotal"].(float64)
	failedAfter := afterJobs["failedTotal"].(float64)
	submittedAfter := afterJobs["submittedTotal"].(float64)

	if completedAfter != completedBefore+1 {
		t.Fatalf("expected completedTotal to increase by exactly 1, before=%v after=%v", completedBefore, completedAfter)
	}
	if failedAfter != failedBefore+1 {
		t.Fatalf("expected failedTotal to increase by exactly 1, before=%v after=%v", failedBefore, failedAfter)
	}
	if submittedAfter < 2 {
		t.Fatalf("expected submittedTotal to reflect both jobs, got %v", submittedAfter)
	}
}
