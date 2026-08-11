package server

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lraigosov/LocaQL/internal/capabilities"
	"github.com/lraigosov/LocaQL/internal/version"
)

// init forces the process's ambient timezone to UTC. Real BigQuery is
// UTC-native for DATE/DATETIME/TIMESTAMP; this codebase already calls
// .UTC() explicitly everywhere it constructs a time.Time itself, but the
// embedded query engine's own SQL text handling (DATE 'YYYY-MM-DD' literal
// parsing, CAST(... AS DATE)) consults the ambient process timezone
// somewhere it can't be intercepted from here, and on a host configured
// with a negative UTC offset that silently produced a date one calendar
// day earlier than the literal says. Verified empirically: with the host
// left on its real timezone the bug reproduces exactly as before; forcing
// time.Local = time.UTC here makes it disappear regardless of what
// timezone the host machine is actually configured with (see
// TestDateStringLiteralParsesCorrectlyRegardlessOfHostTimezone).
func init() {
	time.Local = time.UTC
}

type Server struct {
	mux      *http.ServeMux
	registry capabilities.Registry
	jobs     *jobService
	datasets *datasetService
	tables   *tableService
	routines *routineService
	models   *modelService
	sessions *sessionService
	metrics  *metricsService
	uploads  *bigQueryUploadService
	logger   *slog.Logger

	streamingInserts *streamingInsertDedupStore
	sqlEngines       *sqlEnginePool

	// mergeSourceCounter names the ephemeral tables rewriteMergeUsingSubquery
	// materializes for a MERGE ... USING (<subquery>) whose source the
	// embedded engine can't resolve directly (see that function). Atomic
	// because concurrent MERGE jobs on different targets can both be
	// rewriting at once.
	mergeSourceCounter atomic.Uint64
}

func New(reg capabilities.Registry) *Server {
	s := &Server{
		mux:              http.NewServeMux(),
		registry:         reg,
		jobs:             newJobService(),
		datasets:         newDatasetService(),
		tables:           newTableService(),
		routines:         newRoutineService(),
		models:           newModelService(),
		sessions:         newSessionService(),
		metrics:          newMetricsService(time.Now()),
		uploads:          newBigQueryUploadService(),
		logger:           newDefaultLogger(),
		streamingInserts: newStreamingInsertDedupStore(),
		sqlEngines:       newSQLEnginePool(),
	}
	s.jobs.copyExecutor = s.executeCopyJob
	s.jobs.loadExecutor = s.executeLoadJob
	s.jobs.extractExecutor = s.executeExtractJob
	s.jobs.queryExecutor = s.executeQueryJob
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.withObservability(withGzipRequestDecoding(s.mux))
}

// withGzipRequestDecoding transparently decompresses a request body sent
// with Content-Encoding: gzip before any handler reads it. The official
// Java BigQuery client (google-http-client, used by
// test/clients/java/PersistentDdlDml.java) gzip-compresses POST bodies by
// default as a bandwidth optimization — without this, every handler's
// io.ReadAll(r.Body) + json.Unmarshal reads raw gzip bytes, which fails to
// parse as JSON. Several call sites treat that parse failure as "the field
// was simply omitted" rather than a hard error (a reasonable assumption
// when the body really is malformed JSON, not when it's valid JSON the
// server just never decompressed), so a gzip-compressed jobs.insert
// silently created a job with an empty query text and no jobType override
// instead of executing the real statement or failing loudly — found the
// first time the Java client conformance test ran against a real server.
func withGzipRequestDecoding(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid gzip-encoded request body: "+err.Error(), "invalid")
				return
			}
			defer gz.Close()
			r.Body = gz
			r.Header.Del("Content-Encoding")
			r.ContentLength = -1
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) routes() {
	s.mux.HandleFunc("/_emulator/health", s.health)
	s.mux.HandleFunc("/_emulator/readiness", s.readiness)
	s.mux.HandleFunc("/_emulator/version", s.version)
	s.mux.HandleFunc("/_emulator/capabilities", s.capabilities)
	s.mux.HandleFunc("/_emulator/metrics", s.metricsEndpoint)
	s.mux.HandleFunc("/_emulator/diagnostics", s.diagnosticsEndpoint)
	s.mux.HandleFunc("/_emulator/bigquery/uploads/", s.bigQueryResumableUpload)
	s.mux.HandleFunc("/_emulator/datasets/undelete", s.undeleteDataset)
	s.mux.HandleFunc("/_emulator/workspace/validate", s.workspaceValidate)
	s.mux.HandleFunc("/_emulator/workspace/plan", s.workspacePlan)
	s.mux.HandleFunc("/_emulator/workspace/diff", s.workspaceDiff)
	s.mux.HandleFunc("/_emulator/workspace/apply", s.workspaceApply)
	s.mux.HandleFunc("/$discovery/rest", s.bigQueryDiscoveryDocument)
	s.mux.HandleFunc("/bigquery/v2/projects/", s.bigQueryV2)
	s.mux.HandleFunc("/upload/bigquery/v2/projects/", s.bigQueryJobUpload)
	s.mux.HandleFunc("/storage/v1/b", s.gcsBucketsCollection)
	s.mux.HandleFunc("/storage/v1/b/", s.gcsBucketScope)
	s.mux.HandleFunc("/download/storage/v1/b/", s.gcsDownloadScope)
	s.mux.HandleFunc("/upload/storage/v1/b/", s.gcsObjectUpload)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"service":    version.Name,
		"subsystems": s.subsystemDiagnostics(),
	})
}

func (s *Server) readiness(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ready",
		"service":    version.Name,
		"subsystems": s.subsystemDiagnostics(),
	})
}

func (s *Server) version(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"name":      version.Name,
		"version":   version.Version,
		"commit":    version.Commit,
		"buildDate": version.BuildDate,
	})
}

func (s *Server) capabilities(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.registry)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string, reason string) {
	// Simple mapping for Status string: "Not Found" -> "NOT_FOUND"
	statusStr := strings.ReplaceAll(strings.ToUpper(http.StatusText(status)), " ", "_")
	if statusStr == "" {
		statusStr = "UNKNOWN"
	}

	resp := map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": message,
			"errors": []map[string]any{
				{
					"message": message,
					"domain":  "global",
					"reason":  reason,
				},
			},
			"status": statusStr,
		},
	}
	writeJSON(w, status, resp)
}

func (s *Server) checkETag(w http.ResponseWriter, r *http.Request, version int) bool {
	etag := fmt.Sprintf("\"v%d\"", version)
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	return false
}
