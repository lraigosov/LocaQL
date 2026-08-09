// locaql-bench is a small, dependency-free REST benchmark client for any
// BigQuery-REST-compatible server. It drives the same HTTP workloads
// against whatever --endpoint/--project it is pointed at, so the exact same
// binary produces reproducible results across runs and hardware instead of
// hand-tuned, drifting scripts. See docs/benchmarks.md for methodology,
// published results and their environment.
//
// Usage:
//
//	locaql-bench --endpoint http://127.0.0.1:9050 --project bench --label LocaQL
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type config struct {
	endpoint    string
	project     string
	dataset     string
	label       string
	iterations  int
	insertIters int
	concurrency int
	warmup      int
	jsonOut     string
	timeout     time.Duration
}

func parseConfig() config {
	fs := flag.NewFlagSet("locaql-bench", flag.ExitOnError)
	endpoint := fs.String("endpoint", "http://127.0.0.1:9050", "base URL of the BigQuery-REST-compatible server")
	project := fs.String("project", "bench", "project ID to use (created implicitly by most REST calls)")
	dataset := fs.String("dataset", "bench", "dataset ID to create and populate for the run")
	label := fs.String("label", "server", "human-readable label for this run, shown in the report")
	iterations := fs.Int("iterations", 50, "iterations for each query workload (query round trips are the slow path — see docs/benchmarks.md)")
	insertIters := fs.Int("insert-iterations", 500, "iterations for each streaming-insert workload")
	concurrency := fs.Int("concurrency", 8, "concurrent workers for the concurrent-query workload")
	warmup := fs.Int("warmup", 3, "warmup iterations excluded from reported stats, per workload")
	jsonOut := fs.String("json", "", "optional path to also write the report as JSON")
	timeout := fs.Duration("timeout", 30*time.Second, "per-request HTTP client timeout")
	fs.Parse(os.Args[1:])
	return config{
		endpoint: *endpoint, project: *project, dataset: *dataset, label: *label,
		iterations: *iterations, insertIters: *insertIters, concurrency: *concurrency,
		warmup: *warmup, jsonOut: *jsonOut, timeout: *timeout,
	}
}

type client struct {
	base    string
	project string
	http    *http.Client
}

func newClient(cfg config) *client {
	return &client{base: cfg.endpoint, project: cfg.project, http: &http.Client{Timeout: cfg.timeout}}
}

func (c *client) do(method, path string, body any) ([]byte, int, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return data, resp.StatusCode, err
}

const bigQueryV2Prefix = "/bigquery/v2/projects/"

func (c *client) projectPath(suffix string) string {
	return bigQueryV2Prefix + c.project + suffix
}

func (c *client) createDataset(datasetID string) error {
	body := map[string]any{"datasetReference": map[string]any{"datasetId": datasetID}}
	data, status, err := c.do(http.MethodPost, c.projectPath("/datasets"), body)
	if err != nil {
		return err
	}
	// A pre-existing dataset from a previous run is fine; anything else isn't.
	if status != http.StatusOK && status != http.StatusConflict {
		return fmt.Errorf("createDataset: status %d: %s", status, data)
	}
	return nil
}

func (c *client) createTable(datasetID, tableID string, fields []map[string]any) error {
	body := map[string]any{
		"tableReference": map[string]any{"tableId": tableID},
		"schema":         map[string]any{"fields": fields},
	}
	data, status, err := c.do(http.MethodPost, c.projectPath("/datasets/"+datasetID+"/tables"), body)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusConflict {
		return fmt.Errorf("createTable %s: status %d: %s", tableID, status, data)
	}
	return nil
}

func (c *client) insertAll(datasetID, tableID string, rows []map[string]any) (time.Duration, error) {
	wrapped := make([]any, len(rows))
	for i, r := range rows {
		wrapped[i] = map[string]any{"json": r}
	}
	body := map[string]any{"rows": wrapped}
	start := time.Now()
	data, status, err := c.do(http.MethodPost, c.projectPath("/datasets/"+datasetID+"/tables/"+tableID+"/insertAll"), body)
	elapsed := time.Since(start)
	if err != nil {
		return elapsed, err
	}
	if status != http.StatusOK {
		return elapsed, fmt.Errorf("insertAll: status %d: %s", status, data)
	}
	var out struct {
		InsertErrors []any `json:"insertErrors"`
	}
	if jsonErr := json.Unmarshal(data, &out); jsonErr == nil && len(out.InsertErrors) > 0 {
		return elapsed, fmt.Errorf("insertAll: %d row errors: %s", len(out.InsertErrors), data)
	}
	return elapsed, nil
}

func (c *client) query(sql string) (time.Duration, error) {
	body := map[string]any{"query": sql, "timeoutMs": 30000}
	start := time.Now()
	data, status, err := c.do(http.MethodPost, c.projectPath("/queries"), body)
	elapsed := time.Since(start)
	if err != nil {
		return elapsed, err
	}
	if status != http.StatusOK {
		return elapsed, fmt.Errorf("query: status %d: %s", status, data)
	}
	var out struct {
		JobComplete bool `json:"jobComplete"`
		Errors      any  `json:"errors"`
	}
	if jsonErr := json.Unmarshal(data, &out); jsonErr == nil {
		if out.Errors != nil {
			return elapsed, fmt.Errorf("query returned errors: %v", out.Errors)
		}
		if !out.JobComplete {
			return elapsed, fmt.Errorf("query did not complete within timeoutMs")
		}
	}
	return elapsed, nil
}

// latencies collects samples for one workload and reports percentiles. Not
// safe for concurrent writes without external locking (see workloadConcurrent).
type latencies struct {
	name    string
	samples []time.Duration
	errors  int
}

func (l *latencies) add(d time.Duration) { l.samples = append(l.samples, d) }

func (l *latencies) percentile(p float64) time.Duration {
	if len(l.samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), l.samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (l *latencies) mean() time.Duration {
	if len(l.samples) == 0 {
		return 0
	}
	var total time.Duration
	for _, s := range l.samples {
		total += s
	}
	return total / time.Duration(len(l.samples))
}

func runSequentialWorkload(name string, iterations, warmup int, fn func() (time.Duration, error)) *latencies {
	l := &latencies{name: name}
	for i := 0; i < warmup; i++ {
		_, _ = fn()
	}
	for i := 0; i < iterations; i++ {
		d, err := fn()
		if err != nil {
			l.errors++
			log.Printf("[%s] iteration %d: %v", name, i, err)
			continue
		}
		l.add(d)
	}
	return l
}

func runConcurrentWorkload(name string, total, concurrency int, fn func() (time.Duration, error)) *latencies {
	l := &latencies{name: name}
	var mu sync.Mutex
	var wg sync.WaitGroup
	work := make(chan struct{}, total)
	for i := 0; i < total; i++ {
		work <- struct{}{}
	}
	close(work)

	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range work {
				d, err := fn()
				mu.Lock()
				if err != nil {
					l.errors++
				} else {
					l.add(d)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	wallClock := time.Since(start)
	log.Printf("[%s] %d requests over %d workers in %s (%.1f req/s)", name, total, concurrency, wallClock, float64(total)/wallClock.Seconds())
	return l
}

type report struct {
	Label     string           `json:"label"`
	Endpoint  string           `json:"endpoint"`
	Workloads []workloadReport `json:"workloads"`
}

type workloadReport struct {
	Name   string  `json:"name"`
	Count  int     `json:"count"`
	Errors int     `json:"errors"`
	P50Ms  float64 `json:"p50Ms"`
	P95Ms  float64 `json:"p95Ms"`
	P99Ms  float64 `json:"p99Ms"`
	MeanMs float64 `json:"meanMs"`
	MaxMs  float64 `json:"maxMs"`
	Extra  string  `json:"extra,omitempty"`
}

func toReport(l *latencies, extra string) workloadReport {
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	max := time.Duration(0)
	for _, s := range l.samples {
		if s > max {
			max = s
		}
	}
	return workloadReport{
		Name: l.name, Count: len(l.samples), Errors: l.errors,
		P50Ms: ms(l.percentile(50)), P95Ms: ms(l.percentile(95)), P99Ms: ms(l.percentile(99)),
		MeanMs: ms(l.mean()), MaxMs: ms(max), Extra: extra,
	}
}

func printReportTable(rep report) {
	fmt.Printf("\n=== %s (%s) ===\n", rep.Label, rep.Endpoint)
	fmt.Printf("%-28s %6s %6s %10s %10s %10s %10s  %s\n", "workload", "n", "errs", "p50(ms)", "p95(ms)", "p99(ms)", "mean(ms)", "extra")
	for _, w := range rep.Workloads {
		fmt.Printf("%-28s %6d %6d %10.2f %10.2f %10.2f %10.2f  %s\n", w.Name, w.Count, w.Errors, w.P50Ms, w.P95Ms, w.P99Ms, w.MeanMs, w.Extra)
	}
}

func seedRows(n int, offset int) []map[string]any {
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, map[string]any{"id": offset + i, "label": fmt.Sprintf("row-%d", offset+i), "amount": float64(i) * 1.5})
	}
	return rows
}

func main() {
	cfg := parseConfig()
	c := newClient(cfg)

	eventsFields := []map[string]any{
		{"name": "id", "type": "INT64"},
		{"name": "label", "type": "STRING"},
		{"name": "amount", "type": "FLOAT64"},
	}
	labelsFields := []map[string]any{
		{"name": "id", "type": "INT64"},
		{"name": "category", "type": "STRING"},
	}

	must := func(err error) {
		if err != nil {
			log.Fatalf("setup: %v", err)
		}
	}
	must(c.createDataset(cfg.dataset))
	must(c.createTable(cfg.dataset, "bench_events", eventsFields))
	must(c.createTable(cfg.dataset, "bench_labels", labelsFields))
	for offset := 0; offset < 1000; offset += 200 {
		_, err := c.insertAll(cfg.dataset, "bench_events", seedRows(200, offset))
		must(err)
	}
	labelRows := make([]map[string]any, 0, 500)
	for i := 0; i < 500; i++ {
		labelRows = append(labelRows, map[string]any{"id": i, "category": fmt.Sprintf("cat-%d", i%10)})
	}
	must2 := func(_ time.Duration, err error) { must(err) }
	must2(c.insertAll(cfg.dataset, "bench_labels", labelRows))

	qualifiedEvents := cfg.dataset + ".bench_events"
	qualifiedLabels := cfg.dataset + ".bench_labels"

	rep := report{Label: cfg.label, Endpoint: cfg.endpoint}

	rep.Workloads = append(rep.Workloads, toReport(runSequentialWorkload("sync_query_trivial", cfg.iterations, cfg.warmup, func() (time.Duration, error) {
		return c.query("SELECT 1 AS one")
	}), ""))

	rep.Workloads = append(rep.Workloads, toReport(runSequentialWorkload("sync_query_small_table", cfg.iterations, cfg.warmup, func() (time.Duration, error) {
		return c.query(fmt.Sprintf("SELECT COUNT(*), SUM(amount) FROM %s WHERE id > 500", qualifiedEvents))
	}), ""))

	rep.Workloads = append(rep.Workloads, toReport(runSequentialWorkload("sync_query_join", cfg.iterations, cfg.warmup, func() (time.Duration, error) {
		return c.query(fmt.Sprintf("SELECT e.id, e.label, l.category FROM %s e JOIN %s l ON e.id = l.id WHERE e.id < 100", qualifiedEvents, qualifiedLabels))
	}), ""))

	insertCounter := 100000
	singleInsert := runSequentialWorkload("streaming_insert_single_row", cfg.insertIters, cfg.warmup, func() (time.Duration, error) {
		insertCounter++
		return c.insertAll(cfg.dataset, "bench_events", []map[string]any{{"id": insertCounter, "label": "x", "amount": 1.0}})
	})
	rep.Workloads = append(rep.Workloads, toReport(singleInsert, fmt.Sprintf("%.0f rows/sec", float64(len(singleInsert.samples))/sumSeconds(singleInsert))))

	batchInsert := runSequentialWorkload("streaming_insert_batch_100", cfg.insertIters/10, cfg.warmup, func() (time.Duration, error) {
		insertCounter += 100
		return c.insertAll(cfg.dataset, "bench_events", seedRows(100, insertCounter))
	})
	rep.Workloads = append(rep.Workloads, toReport(batchInsert, fmt.Sprintf("%.0f rows/sec", float64(len(batchInsert.samples)*100)/sumSeconds(batchInsert))))

	rep.Workloads = append(rep.Workloads, toReport(runConcurrentWorkload("concurrent_sync_queries", cfg.iterations, cfg.concurrency, func() (time.Duration, error) {
		return c.query(fmt.Sprintf("SELECT COUNT(*) FROM %s", qualifiedEvents))
	}), fmt.Sprintf("concurrency=%d", cfg.concurrency)))

	printReportTable(rep)

	if cfg.jsonOut != "" {
		f, err := os.Create(cfg.jsonOut)
		must(err)
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		must(enc.Encode(rep))
		fmt.Printf("\nJSON report written to %s\n", cfg.jsonOut)
	}
}

func sumSeconds(l *latencies) float64 {
	var total time.Duration
	for _, s := range l.samples {
		total += s
	}
	if total == 0 {
		return 1
	}
	return total.Seconds()
}
