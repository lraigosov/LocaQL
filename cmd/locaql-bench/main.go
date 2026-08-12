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

	largeTableRows int

	soakDuration       time.Duration
	soakConcurrency    int
	soakMaxOutage      time.Duration
	soakReportInterval time.Duration
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
	largeTableRows := fs.Int("large-table-rows", 0, "if > 0, seed an additional table with this many rows and run aggregation workloads against it (COUNT/SUM WHERE, GROUP BY, tabledata.list) — the scale this project's materialized-table cache targets; see docs/benchmarks.md")
	soakDuration := fs.Duration("soak-duration", 0, "if set, ignore the normal workload report and instead sustain load for this long against a single long-lived server, to validate a supervised process's restart loop over many cycles (see docs/benchmarks.md)")
	soakConcurrency := fs.Int("soak-concurrency", 6, "concurrent workers hammering the server during --soak-duration")
	soakMaxOutage := fs.Duration("soak-max-outage", 60*time.Second, "fail immediately if the server produces no successful response for this long continuously during a soak run — a real, unrecovered outage rather than one restart cycle's brief blip")
	soakReportInterval := fs.Duration("soak-report-interval", 30*time.Second, "how often to log soak progress")
	fs.Parse(os.Args[1:])
	return config{
		endpoint: *endpoint, project: *project, dataset: *dataset, label: *label,
		iterations: *iterations, insertIters: *insertIters, concurrency: *concurrency,
		warmup: *warmup, jsonOut: *jsonOut, timeout: *timeout,
		largeTableRows: *largeTableRows,
		soakDuration:   *soakDuration, soakConcurrency: *soakConcurrency,
		soakMaxOutage: *soakMaxOutage, soakReportInterval: *soakReportInterval,
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

// datasetTablesPath is the tables collection endpoint for one dataset
// (tables.insert/list); tablePath is one specific table's endpoint plus
// suffix (e.g. "/insertAll", "/data") — both centralize the
// "/datasets/{id}/tables" path segment shared by every table-scoped call.
func (c *client) datasetTablesPath(datasetID string) string {
	return c.projectPath("/datasets/" + datasetID + "/tables")
}

func (c *client) tablePath(datasetID, tableID, suffix string) string {
	return c.datasetTablesPath(datasetID) + "/" + tableID + suffix
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
	data, status, err := c.do(http.MethodPost, c.datasetTablesPath(datasetID), body)
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
	data, status, err := c.do(http.MethodPost, c.tablePath(datasetID, tableID, "/insertAll"), body)
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

// tabledataListAll pages through a table's tabledata.list endpoint to
// completion, returning the total row count read back and how long the full
// read took — used by the large-table workload (see runLargeTableWorkloads)
// to measure read-back throughput at scale, separate from query latency.
func (c *client) tabledataListAll(datasetID, tableID string, pageSize int) (int, time.Duration, error) {
	start := time.Now()
	total := 0
	path := c.tablePath(datasetID, tableID, "/data") + fmt.Sprintf("?maxResults=%d", pageSize)
	for {
		data, status, err := c.do(http.MethodGet, path, nil)
		if err != nil {
			return total, time.Since(start), err
		}
		if status != http.StatusOK {
			return total, time.Since(start), fmt.Errorf("tabledata.list: status %d: %s", status, data)
		}
		var out struct {
			Rows          []any  `json:"rows"`
			NextPageToken string `json:"pageToken"`
		}
		if err := json.Unmarshal(data, &out); err != nil {
			return total, time.Since(start), err
		}
		total += len(out.Rows)
		if out.NextPageToken == "" {
			break
		}
		path = c.tablePath(datasetID, tableID, "/data") + fmt.Sprintf("?maxResults=%d&pageToken=%s", pageSize, out.NextPageToken)
	}
	return total, time.Since(start), nil
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

// soakState tracks a soak run's outcome across concurrent workers: not just
// aggregate error counts, but *outage* shape — how many distinct periods of
// continuous failure occurred (each one a restart cycle, if the target is
// running under a supervisor or --self-restart) and how long the longest one
// lasted. That distinction matters here specifically because occasional
// failures are the expected, successfully-mitigated behavior (see
// docs/benchmarks.md's Blocking #3 finding) — a soak run with zero outages
// didn't reproduce the known crash at all this time, which is a valid if
// less informative outcome, while a soak run with one very long outage means
// the restart loop did not actually recover.
type soakState struct {
	mu            sync.Mutex
	attempts      int64
	successes     int64
	failures      int64
	inOutage      bool
	outageStarted time.Time
	outageCount   int
	longestOutage time.Duration
}

func (s *soakState) recordSuccess() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	s.successes++
	if s.inOutage {
		if d := time.Since(s.outageStarted); d > s.longestOutage {
			s.longestOutage = d
		}
		s.inOutage = false
	}
}

func (s *soakState) recordFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	s.failures++
	if !s.inOutage {
		s.inOutage = true
		s.outageStarted = time.Now()
		s.outageCount++
	}
}

// currentOutageDuration reports how long the *current, still-ongoing*
// outage has lasted, or zero if the server is currently healthy — this is
// what --soak-max-outage bounds, checked continuously rather than only at
// the end of the run, so a genuinely stuck server fails the soak promptly
// instead of only after the full --soak-duration elapses.
func (s *soakState) currentOutageDuration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inOutage {
		return 0
	}
	return time.Since(s.outageStarted)
}

type soakSnapshot struct {
	attempts, successes, failures int64
	outageCount                   int
	longestOutage                 time.Duration
}

func (s *soakState) snapshot() soakSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return soakSnapshot{
		attempts: s.attempts, successes: s.successes, failures: s.failures,
		outageCount: s.outageCount, longestOutage: s.longestOutage,
	}
}

// soakWorker repeatedly issues the cheapest possible real query
// (SELECT 1 AS one) as fast as the server answers — no dataset/table
// dependency, since a supervised restart wipes the catalog of a
// non-persistent process, and this exercises exactly the code path that
// KNOWN-DIVERGENCES.md Blocking #3 documents as the crash trigger
// (materializing a fresh isolated engine instance per query via sql.Open).
// A failure is recorded and retried after a short backoff rather than
// treated as fatal — a connection refused/reset here is the expected,
// transient shape of one restart cycle, not a bug in this client.
func soakWorker(stop <-chan struct{}, c *client, state *soakState) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		if _, err := c.query("SELECT 1 AS one"); err != nil {
			state.recordFailure()
			time.Sleep(200 * time.Millisecond)
			continue
		}
		state.recordSuccess()
	}
}

type soakReport struct {
	Label            string  `json:"label"`
	Endpoint         string  `json:"endpoint"`
	DurationSeconds  float64 `json:"durationSeconds"`
	Attempts         int64   `json:"attempts"`
	Successes        int64   `json:"successes"`
	Failures         int64   `json:"failures"`
	ErrorRatePercent float64 `json:"errorRatePercent"`
	OutageCount      int     `json:"outageCount"`
	LongestOutageSec float64 `json:"longestOutageSeconds"`
}

// runSoak sustains load for cfg.soakDuration and validates that the server
// stays *eventually* available throughout — the reliability property this
// project can actually promise given Blocking #3's unresolved upstream root
// cause: an occasional crash is expected and mitigated (pooling, panic
// recovery, process-level auto-restart), not eliminated. It returns an error
// only when the server goes down and stays down longer than
// --soak-max-outage, which is the one outcome that would mean the mitigation
// itself has regressed, not that the known crash happened again.
func runSoak(cfg config, c *client) (soakReport, error) {
	state := &soakState{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < cfg.soakConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			soakWorker(stop, c, state)
		}()
	}

	start := time.Now()
	deadline := start.Add(cfg.soakDuration)
	nextReport := start.Add(cfg.soakReportInterval)
	var failErr error

	for time.Now().Before(deadline) {
		time.Sleep(1 * time.Second)
		if d := state.currentOutageDuration(); d > cfg.soakMaxOutage {
			snap := state.snapshot()
			failErr = fmt.Errorf("soak failed: server unavailable for %s (exceeds --soak-max-outage=%s) after %d attempts (%d successes, %d failures, %d prior outages) — restart loop did not recover in time",
				d.Round(time.Second), cfg.soakMaxOutage, snap.attempts, snap.successes, snap.failures, snap.outageCount)
			break
		}
		if now := time.Now(); !now.Before(nextReport) {
			snap := state.snapshot()
			log.Printf("[soak] %s elapsed: attempts=%d successes=%d failures=%d outages=%d longestOutage=%s",
				now.Sub(start).Round(time.Second), snap.attempts, snap.successes, snap.failures, snap.outageCount, snap.longestOutage.Round(time.Second))
			nextReport = now.Add(cfg.soakReportInterval)
		}
	}

	close(stop)
	wg.Wait()

	snap := state.snapshot()
	errorRate := 0.0
	if snap.attempts > 0 {
		errorRate = float64(snap.failures) / float64(snap.attempts) * 100
	}
	rep := soakReport{
		Label: cfg.label, Endpoint: cfg.endpoint, DurationSeconds: time.Since(start).Seconds(),
		Attempts: snap.attempts, Successes: snap.successes, Failures: snap.failures,
		ErrorRatePercent: errorRate, OutageCount: snap.outageCount, LongestOutageSec: snap.longestOutage.Seconds(),
	}
	log.Printf("[soak] finished: duration=%s attempts=%d successes=%d failures=%d (%.2f%% error rate) outages=%d longestOutage=%s",
		cfg.soakDuration, snap.attempts, snap.successes, snap.failures, errorRate, snap.outageCount, snap.longestOutage.Round(time.Second))
	return rep, failErr
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

// writeJSONReport marshals v as indented JSON to path, used for both the
// normal workload report and the soak report.
func writeJSONReport(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// runSoakCommand runs the soak workload and writes its JSON report if
// requested, returning the soak's own outcome (a real, unrecovered outage)
// separately from a report-writing failure, both surfaced via a single
// error so main can stay a flat, unnested dispatcher.
func runSoakCommand(cfg config, c *client) error {
	rep, soakErr := runSoak(cfg, c)
	if cfg.jsonOut != "" {
		if err := writeJSONReport(cfg.jsonOut, rep); err != nil {
			return fmt.Errorf("write soak report: %w", err)
		}
		fmt.Printf("\nSoak JSON report written to %s\n", cfg.jsonOut)
	}
	return soakErr
}

// must fails the whole run on a setup error that would make every subsequent
// workload's results meaningless anyway (e.g. the dataset/table itself never
// got created) — used by both main's own setup and runLargeTableWorkloads.
func must(err error) {
	if err != nil {
		log.Fatalf("setup: %v", err)
	}
}

// runLargeTableWorkloads seeds a dedicated table with cfg.largeTableRows rows
// and measures aggregation-query latency and read-back throughput at that
// scale — the shape of query this project's materialized-table cache
// (openMaterializedSQLDatabase, see docs/benchmarks.md) targets, as opposed
// to the ~1000-row table the rest of this tool's workloads use. Only run
// when --large-table-rows > 0, since seeding tens of thousands of rows adds
// real time to every run and the default workload set stays fast on
// purpose.
func runLargeTableWorkloads(cfg config, c *client) []workloadReport {
	must(c.createTable(cfg.dataset, "bench_large_events", []map[string]any{
		{"name": "id", "type": "INT64"},
		{"name": "bucket", "type": "STRING"},
		{"name": "amount", "type": "FLOAT64"},
	}))

	const batch = 500
	seedLat := &latencies{name: "large_table_seed_insert"}
	inserted := 0
	for inserted < cfg.largeTableRows {
		n := batch
		if inserted+n > cfg.largeTableRows {
			n = cfg.largeTableRows - inserted
		}
		rows := make([]map[string]any, 0, n)
		for i := 0; i < n; i++ {
			id := inserted + i
			rows = append(rows, map[string]any{"id": id, "bucket": fmt.Sprintf("bucket-%d", id%20), "amount": float64(id%1000) * 1.25})
		}
		d, err := c.insertAll(cfg.dataset, "bench_large_events", rows)
		if err != nil {
			seedLat.errors++
			log.Printf("[large_table_seed_insert] batch at offset %d: %v", inserted, err)
		} else {
			seedLat.add(d)
		}
		inserted += n
	}

	qualified := cfg.dataset + ".bench_large_events"
	reports := []workloadReport{toReport(seedLat, fmt.Sprintf("%d rows total, %.0f rows/sec", inserted, float64(inserted)/sumSeconds(seedLat)))}

	reports = append(reports, toReport(runSequentialWorkload("sync_query_large_table_where", 20, cfg.warmup, func() (time.Duration, error) {
		return c.query(fmt.Sprintf("SELECT COUNT(*), SUM(amount) FROM %s WHERE id > %d", qualified, cfg.largeTableRows/2))
	}), ""))

	reports = append(reports, toReport(runSequentialWorkload("sync_query_large_table_group_by", 20, cfg.warmup, func() (time.Duration, error) {
		return c.query(fmt.Sprintf("SELECT bucket, COUNT(*), AVG(amount) FROM %s GROUP BY bucket ORDER BY bucket", qualified))
	}), ""))

	rowsRead, listDur, err := c.tabledataListAll(cfg.dataset, "bench_large_events", 1000)
	listLat := &latencies{name: "large_table_tabledata_list"}
	if err != nil {
		listLat.errors++
		log.Printf("[large_table_tabledata_list] %v", err)
	} else {
		listLat.add(listDur)
	}
	reports = append(reports, toReport(listLat, fmt.Sprintf("%d rows read back, %.0f rows/sec", rowsRead, float64(rowsRead)/listDur.Seconds())))

	return reports
}

func main() {
	cfg := parseConfig()
	c := newClient(cfg)

	if cfg.soakDuration > 0 {
		if err := runSoakCommand(cfg, c); err != nil {
			log.Fatal(err)
		}
		return
	}

	eventsFields := []map[string]any{
		{"name": "id", "type": "INT64"},
		{"name": "label", "type": "STRING"},
		{"name": "amount", "type": "FLOAT64"},
	}
	labelsFields := []map[string]any{
		{"name": "id", "type": "INT64"},
		{"name": "category", "type": "STRING"},
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

	if cfg.largeTableRows > 0 {
		rep.Workloads = append(rep.Workloads, runLargeTableWorkloads(cfg, c)...)
	}

	printReportTable(rep)

	if cfg.jsonOut != "" {
		must(writeJSONReport(cfg.jsonOut, rep))
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
