package server

import (
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// latencyBucketBoundsMs are the upper bounds (inclusive, milliseconds) of
// the request-latency histogram exposed at GET /_emulator/metrics. A fixed,
// small bucket set (rather than tracked percentiles) keeps this a plain
// counter array — cheap to maintain under a mutex, no decay/sketch
// algorithm needed for a local, single-process emulator.
var latencyBucketBoundsMs = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

// metricsService is the sibling of jobService/sessionService/etc. for HTTP
// request metrics, backing GET /_emulator/metrics — a plain JSON snapshot
// (see the format-choice note in devlog.md Sesión 80) rather than
// Prometheus text exposition, consistent with the rest of this REST API.
type metricsService struct {
	mu             sync.Mutex
	startedAt      time.Time
	requestsTotal  int64
	statusClass1xx int64
	statusClass2xx int64
	statusClass3xx int64
	statusClass4xx int64
	statusClass5xx int64
	byRouteGroup   map[string]int64
	latencyBuckets []int64 // len == len(latencyBucketBoundsMs)+1, last slot is the +Inf overflow bucket
}

func newMetricsService(now time.Time) *metricsService {
	return &metricsService{
		startedAt:      now,
		byRouteGroup:   make(map[string]int64),
		latencyBuckets: make([]int64, len(latencyBucketBoundsMs)+1),
	}
}

func (m *metricsService) recordRequest(routeGroup string, status int, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.requestsTotal++
	switch {
	case status >= 500:
		m.statusClass5xx++
	case status >= 400:
		m.statusClass4xx++
	case status >= 300:
		m.statusClass3xx++
	case status >= 200:
		m.statusClass2xx++
	default:
		m.statusClass1xx++
	}
	m.byRouteGroup[routeGroup]++

	ms := float64(d.Microseconds()) / 1000.0
	idx := len(latencyBucketBoundsMs)
	for i, bound := range latencyBucketBoundsMs {
		if ms <= bound {
			idx = i
			break
		}
	}
	m.latencyBuckets[idx]++
}

func (m *metricsService) snapshot() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()

	buckets := make([]map[string]any, len(m.latencyBuckets))
	for i, count := range m.latencyBuckets {
		label := "+Inf"
		if i < len(latencyBucketBoundsMs) {
			label = strconv.FormatFloat(latencyBucketBoundsMs[i], 'f', -1, 64)
		}
		buckets[i] = map[string]any{"leMs": label, "count": count}
	}
	byRouteGroup := make(map[string]int64, len(m.byRouteGroup))
	for k, v := range m.byRouteGroup {
		byRouteGroup[k] = v
	}

	return map[string]any{
		"total": m.requestsTotal,
		"byStatusClass": map[string]int64{
			"1xx": m.statusClass1xx,
			"2xx": m.statusClass2xx,
			"3xx": m.statusClass3xx,
			"4xx": m.statusClass4xx,
			"5xx": m.statusClass5xx,
		},
		"byRouteGroup":     byRouteGroup,
		"latencyMsBuckets": buckets,
	}
}

// classifyRouteGroup buckets a request path into the same top-level groups
// routes() registers handlers under, so /_emulator/metrics can report load
// per API surface without exploding into one entry per distinct path.
func classifyRouteGroup(path string) string {
	switch {
	case len(path) >= len("/_emulator") && path[:len("/_emulator")] == "/_emulator":
		return "emulator"
	case len(path) >= len("/bigquery/v2") && path[:len("/bigquery/v2")] == "/bigquery/v2":
		return "bigquery"
	case len(path) >= len("/upload/storage/v1") && path[:len("/upload/storage/v1")] == "/upload/storage/v1":
		return "storage_upload"
	case len(path) >= len("/storage/v1") && path[:len("/storage/v1")] == "/storage/v1":
		return "storage"
	default:
		return "other"
	}
}

// statusCapturingResponseWriter records the status code an inner handler
// wrote, defaulting to 200 for a handler that never calls WriteHeader
// explicitly (matching net/http's own default).
type statusCapturingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusCapturingResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// withObservability wraps every route with request-metrics recording and
// structured (log/slog) request logging. Successful requests log at Debug
// (silent at the default Info level, keeping test/normal-operation output
// quiet); 4xx logs at Warn and 5xx at Error, so problems surface by default
// without needing a verbose flag first.
func (s *Server) withObservability(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusCapturingResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		duration := time.Since(start)
		routeGroup := classifyRouteGroup(r.URL.Path)
		s.metrics.recordRequest(routeGroup, sw.status, duration)

		level := slog.LevelDebug
		switch {
		case sw.status >= 500:
			level = slog.LevelError
		case sw.status >= 400:
			level = slog.LevelWarn
		}
		s.logger.Log(r.Context(), level, "http_request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration_ms", duration.Milliseconds(),
			"route_group", routeGroup,
		)
	})
}

func newDefaultLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, nil))
}

// metricsEndpoint serves GET /_emulator/metrics: a JSON snapshot of request
// and job metrics. See the format-choice note in devlog.md Sesión 80 for
// why this is plain JSON (consistent with the rest of this REST API, no new
// dependency) rather than Prometheus text exposition — a real, documented
// limitation for anyone wanting to point real Prometheus/Grafana at this
// emulator directly.
func (s *Server) metricsEndpoint(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"uptimeSeconds": time.Since(s.metrics.startedAt).Seconds(),
		"requests":      s.metrics.snapshot(),
		"jobs":          s.jobs.metricsSnapshot(),
		"sessions":      map[string]any{"active": s.sessions.countAll()},
	})
}

// subsystemDiagnostics backs the "subsystems" field GET /_emulator/health
// and /_emulator/readiness both now include: structural, real diagnostic
// detail (queue depth/capacity, active session count) rather than a
// fabricated health score — there is no failure condition modeled here that
// would ever flip status away from "ok"/"ready", since nothing in this
// emulator has a real degraded state to report yet (see KNOWN-DIVERGENCES.md).
func (s *Server) subsystemDiagnostics() map[string]any {
	return map[string]any{
		"jobs":     s.jobs.metricsSnapshot(),
		"sessions": map[string]any{"active": s.sessions.countAll()},
	}
}
