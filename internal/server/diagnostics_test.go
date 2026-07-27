package server

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestDiagnosticsReportsRecentJobFailures(t *testing.T) {
	s := newTestServer()
	runJobAndFetch(t, s, `{"configuration":{"query":{"query":"SELECT 1 AS one"}}}`)
	runJobAndFetch(t, s, `{"configuration":{"query":{"query":"SELECT FORCE_ERROR"}}}`)

	code, out := getJSON(t, s, "/_emulator/diagnostics")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	failures, ok := out["recentJobFailures"].([]any)
	if !ok || len(failures) == 0 {
		t.Fatalf("expected at least 1 recent job failure, got %v", out["recentJobFailures"])
	}
	first := failures[0].(map[string]any)
	if reason, _ := first["errorReason"].(string); reason == "" {
		t.Fatalf("expected a non-empty errorReason, got %v", first)
	}
	if jobType, _ := first["jobType"].(string); jobType != "query" {
		t.Fatalf("expected jobType=query, got %v", first["jobType"])
	}
}

func TestDiagnosticsReportsPersistenceStatus(t *testing.T) {
	s := newTestServer()

	code, out := getJSON(t, s, "/_emulator/diagnostics")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	persistence := out["persistence"].(map[string]any)
	if enabled, _ := persistence["enabled"].(bool); enabled {
		t.Fatalf("expected persistence disabled by default in newTestServer, got %v", persistence)
	}

	// Force a real persistence failure: point persistencePath under a path
	// component that is a regular file, not a directory, so MkdirAll fails.
	baseFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(baseFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocking file: %v", err)
	}
	badPath := filepath.Join(baseFile, "subdir", "state.json")
	s.jobs.persistencePath = badPath

	runJobAndFetch(t, s, `{"configuration":{"query":{"query":"SELECT 1 AS one"}}}`)

	_, out2 := getJSON(t, s, "/_emulator/diagnostics")
	persistence2 := out2["persistence"].(map[string]any)
	if enabled, _ := persistence2["enabled"].(bool); !enabled {
		t.Fatalf("expected persistence enabled once persistencePath is set, got %v", persistence2)
	}
	if path, _ := persistence2["path"].(string); path != badPath {
		t.Fatalf("expected path=%q, got %v", badPath, persistence2["path"])
	}
	if lastErr, _ := persistence2["lastError"].(string); lastErr == "" {
		t.Fatalf("expected a non-empty lastError after a forced MkdirAll failure, got %v", persistence2)
	}
}

func TestDiagnosticsReportsResourceLocksShape(t *testing.T) {
	s := newTestServer()
	code, out := getJSON(t, s, "/_emulator/diagnostics")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	resourceLocks, ok := out["resourceLocks"].(map[string]any)
	if !ok {
		t.Fatalf("expected a resourceLocks object, got %v", out["resourceLocks"])
	}
	if _, ok := resourceLocks["held"].([]any); !ok {
		t.Fatalf("expected resourceLocks.held to be an array, got %v", resourceLocks["held"])
	}
	if _, ok := resourceLocks["total"]; !ok {
		t.Fatalf("expected resourceLocks.total, got %v", resourceLocks)
	}
}

func TestDiagnosticsReportsActiveSessionCount(t *testing.T) {
	s := newTestServer()
	runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT 1","createSession":true}`)

	_, out := getJSON(t, s, "/_emulator/diagnostics")
	sessions, ok := out["sessions"].(map[string]any)
	if !ok {
		t.Fatalf("expected a sessions object, got %v", out["sessions"])
	}
	if active, _ := sessions["active"].(float64); active < 1 {
		t.Fatalf("expected at least 1 active session, got %v", sessions["active"])
	}
}

func TestDiagnosticsReportsSetEnvironmentVariablesOnly(t *testing.T) {
	t.Setenv("LOCAQL_JOB_WORKERS", "4")

	s := newTestServer()
	_, out := getJSON(t, s, "/_emulator/diagnostics")
	env, ok := out["environment"].(map[string]any)
	if !ok {
		t.Fatalf("expected an environment object, got %v", out["environment"])
	}
	if v, _ := env["LOCAQL_JOB_WORKERS"].(string); v != "4" {
		t.Fatalf("expected LOCAQL_JOB_WORKERS=4, got %v", env["LOCAQL_JOB_WORKERS"])
	}
	if _, present := env["LOCAQL_FAKE_GCS_ROOT"]; present {
		t.Fatalf("expected an unset env var to be absent, not present as empty, got %v", env)
	}
}
