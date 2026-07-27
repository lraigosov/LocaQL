package server

import (
	"net/http"
	"os"
	"strings"
)

// diagnosticsEnvVars are every LOCAQL_* environment variable that changes
// this emulator's behavior, checked at request time (not cached at startup,
// so an operator can see the value actually in effect right now). Only
// variables that are actually set are included in the response — an unset
// one is silently using its documented default, and listing it as empty
// would misleadingly suggest "explicitly configured to nothing".
var diagnosticsEnvVars = []string{
	"LOCAQL_JOB_WORKERS",
	"LOCAQL_STORAGE_WRITE_WORKERS",
	"LOCAQL_FAKE_GCS_ROOT",
	"LOCAQL_EXTRACT_SHARD_MAX_BYTES",
	"LOCAQL_SESSION_IDLE_TIMEOUT_SECONDS",
}

func diagnosticsEnvironment() map[string]string {
	out := map[string]string{}
	for _, name := range diagnosticsEnvVars {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			out[name] = v
		}
	}
	return out
}

// diagnosticsEndpoint serves GET /_emulator/diagnostics: a "why is this
// broken" aggregation distinct from /_emulator/metrics' raw counters —
// persistence write health (previously invisible: every persistLocked()
// call site already discarded its error), the actual failed jobs behind a
// failedTotal count instead of just the number, which specific resource
// keys are contended (not just how many), active sessions, and the
// LOCAQL_* environment variables actually in effect. Real, aggregated data
// from the live catalog — not a fabricated health score, and not the
// catalog-wide dataset/table/routine/model audit that would be a separate,
// larger feature.
func (s *Server) diagnosticsEndpoint(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"persistence":       s.jobs.persistenceStatus(),
		"recentJobFailures": s.jobs.recentFailures(20),
		"resourceLocks": map[string]any{
			"held":  nonNilStrings(s.jobs.resourceLockKeysHeld()),
			"total": s.jobs.metricsSnapshot()["resourceLocksTotal"],
		},
		"sessions":    map[string]any{"active": s.sessions.countAll()},
		"environment": diagnosticsEnvironment(),
	})
}
