package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// TestSharedQueryCacheReflectsTableVersionChanges is the end-to-end version
// of the correctness rule sql_engine_pool.go depends on: a table's Version
// (tableRecord.Version) is bumped by every mutation, so a signature computed
// from the OLD version must never satisfy a read that runs after the bump.
// Without this, a warm shared connection would silently keep serving rows
// materialized before the mutation — exactly the class of bug caught for
// external tables by TestExternalTableQueryReflectsLiveFileContents, here
// exercised for a plain managed table going through the new cache instead of
// bypassing it.
func TestSharedQueryCacheReflectsTableVersionChanges(t *testing.T) {
	s := newTestServer()
	createStreamingInsertTestTable(t, s, "analytics", "cache_ver_t", []map[string]any{
		{"name": "id", "type": "INT64"},
	})
	code, out := insertAllTestRequest(t, s, "analytics", "cache_ver_t", map[string]any{
		"rows": []any{
			map[string]any{"json": map[string]any{"id": 1}},
			map[string]any{"json": map[string]any{"id": 2}},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("seed insertAll: expected 200, got %d: %v", code, out)
	}

	countRows := func() int {
		result, err := s.executeQueryStatement("p1", "", "SELECT id FROM analytics.cache_ver_t ORDER BY id", "", "", nil)
		if err != nil {
			t.Fatalf("query cache_ver_t: %v", err)
		}
		return len(result.rows)
	}

	if got := countRows(); got != 2 {
		t.Fatalf("expected 2 rows before any repeat query, got %d", got)
	}
	// Repeat the identical read-only query: this is the warm-cache-hit path
	// (same signature as before, nothing changed) and must return the exact
	// same result, not stale or duplicated rows.
	if got := countRows(); got != 2 {
		t.Fatalf("expected 2 rows on a repeated (potentially cache-hit) query, got %d", got)
	}

	code, out = insertAllTestRequest(t, s, "analytics", "cache_ver_t", map[string]any{
		"rows": []any{map[string]any{"json": map[string]any{"id": 3}}},
	})
	if code != http.StatusOK {
		t.Fatalf("second insertAll: expected 200, got %d: %v", code, out)
	}

	if got := countRows(); got != 3 {
		t.Fatalf("expected 3 rows after a mutation bumped the table's version, got %d — a stale cache entry keyed on the old version was served", got)
	}
}

// TestWildcardCacheReconciliationDropsStaleTableFromRepurposedEngine
// reproduces the exact hazard reconcileSharedEngine exists to prevent: the
// embedded engine (goccy/googlesqlite) resolves a `dataset.prefix*` query
// natively against its OWN catalog (see expandWildcardTableRefs' doc
// comment), by scanning for every table it currently holds whose name
// matches the prefix — not just the ones this project's own catalog says
// should currently match. If a pooled connection that previously
// materialized a table is reused for a different signature without first
// dropping that table, a later wildcard query against the very same
// connection would wrongly pull the stale table into its UNION ALL, even
// though this project's own catalog no longer has it.
func TestWildcardCacheReconciliationDropsStaleTableFromRepurposedEngine(t *testing.T) {
	s := newTestServer()
	createWildcardShard(t, s, "analytics", "cwc_shard_old", 1, "old")

	firstOut := runQueryAndFetchResults(t, s, "SELECT id FROM `analytics.cwc_shard_*`")
	firstRows, _ := firstOut["rows"].([]any)
	if len(firstRows) != 1 {
		t.Fatalf("expected 1 row from the first wildcard query, got %d: %+v", len(firstRows), firstOut)
	}

	// Remove cwc_shard_old from this project's own catalog. The pooled
	// connection that materialized it is untouched by this — it is a
	// LocaQL-catalog-only operation — so the embedded engine's own
	// sqlite-level table for cwc_shard_old still physically exists on
	// whichever connection served the query above.
	delReq := httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/p1/datasets/analytics/tables/cwc_shard_old", nil)
	delRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(delRes, delReq)
	if delRes.Code != http.StatusNoContent {
		t.Fatalf("delete cwc_shard_old: expected 204, got %d: %s", delRes.Code, delRes.Body.String())
	}

	createWildcardShard(t, s, "analytics", "cwc_shard_new", 2, "new")

	// This project's own catalog now says the wildcard matches exactly one
	// table (cwc_shard_new) — a different signature than the first query's,
	// so with only one pooled connection ever opened by this test, acquiring
	// it must repurpose (and reconcile) rather than hit. If reconciliation
	// is skipped, the embedded engine still natively matches cwc_shard_old
	// too, and the result below comes back with 2 rows instead of 1.
	secondOut := runQueryAndFetchResults(t, s, "SELECT id FROM `analytics.cwc_shard_*`")
	secondRows, _ := secondOut["rows"].([]any)
	if len(secondRows) != 1 {
		t.Fatalf("expected exactly 1 row (cwc_shard_new only) after cwc_shard_old was deleted, got %d: %+v — a stale materialized table leaked through an unreconciled repurposed connection", len(secondRows), secondOut)
	}
	cell := secondRows[0].(map[string]any)["f"].([]any)[0].(map[string]any)["v"]
	if cell != "2" {
		t.Fatalf("expected the surviving row to be id=2 (cwc_shard_new), got %v", cell)
	}
}

// TestPartitionPrunedQueryNeverPollutesTheSharedCacheForAFullTableRead
// exercises the other disqualifying condition resolveTableForMaterialization
// enforces: a partition-pruned materialization is a subset of the table, so
// it must never be cached under a signature a later, unfiltered query for
// the same table could also compute — that would make the full read
// silently return only the pruned subset.
func TestPartitionPrunedQueryNeverPollutesTheSharedCacheForAFullTableRead(t *testing.T) {
	s := newTestServer()
	createPartitionedTable(t, s, "cache_prune_daily", `{
  "tableReference":{"tableId":"cache_prune_daily"},
  "schema":{"fields":[{"name":"event_date","type":"DATE"},{"name":"user_id","type":"STRING"}]},
  "timePartitioning":{"type":"DAY","field":"event_date"}
}`)
	table, _, _ := s.tables.get("p1", "analytics", "cache_prune_daily")
	rows := [][]string{{"2026-08-05", "a"}, {"2026-08-05", "b"}, {"2026-08-06", "c"}}
	if _, err := s.tables.upsertCopyDestination(tableReference{ProjectID: "p1", DatasetID: "analytics", TableID: "cache_prune_daily"}, table.Schema, rows, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append rows: %v", err)
	}

	pruned, err := s.executeQueryStatement("p1", "", "SELECT user_id FROM analytics.cache_prune_daily WHERE event_date = DATE '2026-08-05' ORDER BY user_id", "", "", nil)
	if err != nil {
		t.Fatalf("pruned query: %v", err)
	}
	if len(pruned.rows) != 2 {
		t.Fatalf("expected 2 rows from the pruned partition, got %d", len(pruned.rows))
	}

	full, err := s.executeQueryStatement("p1", "", "SELECT user_id FROM analytics.cache_prune_daily ORDER BY user_id", "", "", nil)
	if err != nil {
		t.Fatalf("full query: %v", err)
	}
	if len(full.rows) != 3 {
		t.Fatalf("expected all 3 rows on the unfiltered read that followed a pruned one, got %d — the pruned subset leaked into the cache", len(full.rows))
	}

	// And the reverse order: a full read followed by a pruned one must
	// still correctly narrow, not reuse the full read's cached materialization.
	prunedAgain, err := s.executeQueryStatement("p1", "", "SELECT user_id FROM analytics.cache_prune_daily WHERE event_date = DATE '2026-08-06' ORDER BY user_id", "", "", nil)
	if err != nil {
		t.Fatalf("second pruned query: %v", err)
	}
	if len(prunedAgain.rows) != 1 || prunedAgain.rows[0][0] != "c" {
		t.Fatalf("expected exactly the single 2026-08-06 row, got %v", prunedAgain.rows)
	}
}

// insertWithOptimisticRetry runs one INSERT, retrying on the pre-existing
// "changed concurrently; retry the statement" optimistic-concurrency error
// every ...IfVersion mutation path already returns (tables_service.go) when
// another goroutine's mutation lands first — this is unrelated to the new
// cache and is exactly what a real client is expected to do on that error.
func insertWithOptimisticRetry(t *testing.T, s *Server, query string) {
	t.Helper()
	for attempt := 0; attempt < 20; attempt++ {
		_, err := s.executeQueryStatement("p1", "", query, "", "", nil)
		if err == nil {
			return
		}
		if !strings.Contains(err.Error(), "changed concurrently") {
			t.Errorf("insert %q: %v", query, err)
			return
		}
	}
	t.Errorf("insert %q: gave up after repeated concurrent-modification conflicts", query)
}

// TestConcurrentReadsAndMutationsAgainstTheSameTableStayCorrect drives real
// concurrent read-only queries and mutating INSERTs against one table
// through the actual HTTP handler stack. It exists to be run with
// CGO_ENABLED=1 go test -race, which is what actually proves a mutating
// statement's always-private connection is never one a concurrent shared
// reader is also touching — a plain sequential test cannot exercise that
// interleaving at all. Correctness is checked too: the final row count must
// equal exactly what was inserted, with no row lost or duplicated.
func TestConcurrentReadsAndMutationsAgainstTheSameTableStayCorrect(t *testing.T) {
	s := newTestServer()
	createStreamingInsertTestTable(t, s, "analytics", "cache_concurrent_t", []map[string]any{
		{"name": "id", "type": "INT64"},
	})

	const inserters = 8
	const readersPerInserter = 4
	var wg sync.WaitGroup
	for i := 0; i < inserters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			insertWithOptimisticRetry(t, s, fmt.Sprintf("INSERT INTO analytics.cache_concurrent_t (id) VALUES (%d)", id))
		}(i)
		for j := 0; j < readersPerInserter; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := s.executeQueryStatement("p1", "", "SELECT COUNT(*) FROM analytics.cache_concurrent_t", "", "", nil); err != nil {
					t.Errorf("concurrent read: %v", err)
				}
			}()
		}
	}
	wg.Wait()

	final, err := s.executeQueryStatement("p1", "", "SELECT id FROM analytics.cache_concurrent_t ORDER BY id", "", "", nil)
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	if len(final.rows) != inserters {
		t.Fatalf("expected exactly %d rows after %d concurrent inserts, got %d: %v", inserters, inserters, len(final.rows), final.rows)
	}
}
