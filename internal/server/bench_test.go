package server

// Benchmarks exercise the real HTTP handler stack (httptest against
// s.Handler()), the same way this project's own tests do, rather than
// calling internal functions directly — the number that matters is what a
// real client sees over REST, not a microbenchmark of one internal call.
// Run with: go test -bench=. -benchmem -run '^$' ./internal/server/...
// (see `make bench`). These are LocaQL-only, local-iteration signals; the
// reproducible comparison against goccy/bigquery-emulator lives in
// cmd/locaql-bench and docs/benchmarks.md, since that requires driving two
// separate real server processes over the network, not two in-process
// httptest handlers.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func createBenchDataset(b *testing.B, s *Server) {
	b.Helper()
	body, err := json.Marshal(map[string]any{"datasetReference": map[string]any{"datasetId": "bench"}})
	if err != nil {
		b.Fatalf("marshal dataset body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		b.Fatalf("create bench dataset: expected 200, got %d: %s", res.Code, res.Body.String())
	}
}

func benchmarkServerWithTable(b *testing.B, rowCount int) *Server {
	b.Helper()
	s := newTestServer()
	createBenchDataset(b, s)
	fields := []map[string]any{
		{"name": "id", "type": "INT64"},
		{"name": "label", "type": "STRING"},
		{"name": "amount", "type": "FLOAT64"},
	}
	body, err := json.Marshal(map[string]any{
		"tableReference": map[string]any{"tableId": "bench_events"},
		"schema":         map[string]any{"fields": fields},
	})
	if err != nil {
		b.Fatalf("marshal table body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/bench/tables", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		b.Fatalf("create bench table: expected 200, got %d: %s", res.Code, res.Body.String())
	}

	rows := make([]any, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		rows = append(rows, map[string]any{"json": map[string]any{
			"id": i, "label": fmt.Sprintf("row-%d", i), "amount": float64(i) * 1.5,
		}})
	}
	if rowCount > 0 {
		insertBody, err := json.Marshal(map[string]any{"rows": rows})
		if err != nil {
			b.Fatalf("marshal insertAll body: %v", err)
		}
		insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/bench/tables/bench_events/insertAll", strings.NewReader(string(insertBody)))
		insertReq.Header.Set("Content-Type", "application/json")
		insertRes := httptest.NewRecorder()
		s.Handler().ServeHTTP(insertRes, insertReq)
		if insertRes.Code != http.StatusOK {
			b.Fatalf("seed bench table: expected 200, got %d: %s", insertRes.Code, insertRes.Body.String())
		}
	}
	return s
}

func runBenchQuery(b *testing.B, s *Server, query string) {
	b.Helper()
	body, err := json.Marshal(map[string]any{"query": query})
	if err != nil {
		b.Fatalf("marshal query body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		b.Fatalf("query %q: expected 200, got %d: %s", query, res.Code, res.Body.String())
	}
}

// BenchmarkSyncQueryTrivial measures the floor cost of one round trip through
// the sync /queries endpoint for a query that touches no catalog table.
func BenchmarkSyncQueryTrivial(b *testing.B) {
	s := newTestServer()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		runBenchQuery(b, s, "SELECT 1 AS one")
	}
}

// BenchmarkSyncQuerySmallTable measures a WHERE + aggregate over a small,
// realistic table (1k rows) — the shape of query a developer runs
// interactively while iterating on a pipeline locally.
func BenchmarkSyncQuerySmallTable(b *testing.B) {
	s := benchmarkServerWithTable(b, 1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		runBenchQuery(b, s, "SELECT COUNT(*), SUM(amount) FROM bench.bench_events WHERE id > 500")
	}
}

// BenchmarkSyncQueryJoin measures a join between two materialized tables —
// the path that most exercises openMaterializedSQLDatabase with more than
// one referenced table.
func BenchmarkSyncQueryJoin(b *testing.B) {
	s := benchmarkServerWithTable(b, 500)
	labelsFields := []map[string]any{
		{"name": "id", "type": "INT64"},
		{"name": "category", "type": "STRING"},
	}
	labelsBody, _ := json.Marshal(map[string]any{
		"tableReference": map[string]any{"tableId": "bench_labels"},
		"schema":         map[string]any{"fields": labelsFields},
	})
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/bench/tables", strings.NewReader(string(labelsBody)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		b.Fatalf("create bench_labels: expected 200, got %d: %s", res.Code, res.Body.String())
	}
	rows := make([]any, 0, 500)
	for i := 0; i < 500; i++ {
		rows = append(rows, map[string]any{"json": map[string]any{"id": i, "category": fmt.Sprintf("cat-%d", i%10)}})
	}
	insertBody, _ := json.Marshal(map[string]any{"rows": rows})
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/bench/tables/bench_labels/insertAll", strings.NewReader(string(insertBody)))
	insertReq.Header.Set("Content-Type", "application/json")
	insertRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertRes, insertReq)
	if insertRes.Code != http.StatusOK {
		b.Fatalf("seed bench_labels: expected 200, got %d: %s", insertRes.Code, insertRes.Body.String())
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		runBenchQuery(b, s, "SELECT e.id, e.label, l.category FROM bench.bench_events e JOIN bench.bench_labels l ON e.id = l.id WHERE e.id < 100")
	}
}

// BenchmarkStreamingInsertSingleRow measures the per-request cost of one
// tabledata.insertAll call carrying a single row — the direct, job-free
// write path (see streaming_inserts.go).
func BenchmarkStreamingInsertSingleRow(b *testing.B) {
	s := benchmarkServerWithTable(b, 0)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		body, _ := json.Marshal(map[string]any{
			"rows": []any{map[string]any{"json": map[string]any{"id": i, "label": "x", "amount": 1.0}}},
		})
		req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/bench/tables/bench_events/insertAll", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		s.Handler().ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			b.Fatalf("insertAll: expected 200, got %d: %s", res.Code, res.Body.String())
		}
	}
}

// BenchmarkStreamingInsertBatch100 measures throughput (rows/op via
// b.ReportMetric) for a single insertAll call carrying 100 rows at once —
// the batched shape most real client libraries actually send.
func BenchmarkStreamingInsertBatch100(b *testing.B) {
	const batchSize = 100
	s := benchmarkServerWithTable(b, 0)
	rows := make([]any, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		rows = append(rows, map[string]any{"json": map[string]any{"id": i, "label": "x", "amount": 1.0}})
	}
	body, _ := json.Marshal(map[string]any{"rows": rows})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/bench/tables/bench_events/insertAll", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		s.Handler().ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			b.Fatalf("insertAll batch: expected 200, got %d: %s", res.Code, res.Body.String())
		}
	}
	b.ReportMetric(float64(batchSize*b.N)/b.Elapsed().Seconds(), "rows/sec")
}

// BenchmarkConcurrentSyncQueries measures throughput under concurrent load —
// the queue/backpressure path (jobService.resourceSlots/runSlots) rather
// than single-request latency.
func BenchmarkConcurrentSyncQueries(b *testing.B) {
	s := benchmarkServerWithTable(b, 200)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			runBenchQuery(b, s, "SELECT COUNT(*) FROM bench.bench_events")
		}
	})
}
