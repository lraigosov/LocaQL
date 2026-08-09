package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// createWildcardShard creates one real, standard table via tables.insert and
// inserts a single row into it via tabledata.insertAll — the same building
// blocks streaming_inserts_test.go already exercises, reused here to set up
// a small family of sharded tables sharing a common name prefix.
func createWildcardShard(t *testing.T, s *Server, datasetID, tableID string, id int, label string) {
	t.Helper()
	createStreamingInsertTestTable(t, s, datasetID, tableID, []map[string]any{
		{"name": "id", "type": "INT64"},
		{"name": "label", "type": "STRING"},
	})
	code, out := insertAllTestRequest(t, s, datasetID, tableID, map[string]any{
		"rows": []any{map[string]any{"json": map[string]any{"id": id, "label": label}}},
	})
	if code != http.StatusOK {
		t.Fatalf("insertAll into %s: expected 200, got %d", tableID, code)
	}
	if _, hasErrors := out["insertErrors"]; hasErrors {
		t.Fatalf("insertAll into %s: unexpected insertErrors: %v", tableID, out["insertErrors"])
	}
}

func runQueryAndFetchResults(t *testing.T, s *Server, query string) map[string]any {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"query": query})
	if err != nil {
		t.Fatalf("marshal query body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("sync query: expected 200, got %d: %s", res.Code, res.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode query results: %v", err)
	}
	if out["errors"] != nil {
		t.Fatalf("unexpected query error: %v", out["errors"])
	}
	return out
}

func TestWildcardTableUnionsMatchingTablesWithTableSuffix(t *testing.T) {
	s := newTestServer()
	createWildcardShard(t, s, "analytics", "wc_events_20260101", 1, "a")
	createWildcardShard(t, s, "analytics", "wc_events_20260102", 2, "b")
	// A table that merely contains the prefix as a substring elsewhere, or an
	// unrelated table, must never be pulled into the union.
	createWildcardShard(t, s, "analytics", "wc_other", 99, "z")

	out := runQueryAndFetchResults(t, s, "SELECT id, label, _TABLE_SUFFIX AS suffix FROM `analytics.wc_events_*` ORDER BY id")
	rows, _ := out["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("expected 2 unioned rows, got %d: %+v", len(rows), out)
	}

	first := rows[0].(map[string]any)["f"].([]any)
	if got := first[2].(map[string]any)["v"]; got != "20260101" {
		t.Fatalf("expected _TABLE_SUFFIX=20260101 on first row, got %v", got)
	}
	second := rows[1].(map[string]any)["f"].([]any)
	if got := second[2].(map[string]any)["v"]; got != "20260102" {
		t.Fatalf("expected _TABLE_SUFFIX=20260102 on second row, got %v", got)
	}
}

func TestWildcardTableSuffixFilterNarrowsToOneShard(t *testing.T) {
	s := newTestServer()
	createWildcardShard(t, s, "analytics", "wcf_events_20260101", 1, "a")
	createWildcardShard(t, s, "analytics", "wcf_events_20260102", 2, "b")

	out := runQueryAndFetchResults(t, s, "SELECT id FROM `analytics.wcf_events_*` WHERE _TABLE_SUFFIX = '20260102'")
	rows, _ := out["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 row after _TABLE_SUFFIX filter, got %d: %+v", len(rows), out)
	}
	cell := rows[0].(map[string]any)["f"].([]any)[0].(map[string]any)["v"]
	if cell != "2" {
		t.Fatalf("expected id=2, got %v", cell)
	}
}

func TestWildcardTableExcludesViewsAndExternalTables(t *testing.T) {
	s := newTestServer()
	createWildcardShard(t, s, "analytics", "wcv_events_20260101", 1, "a")

	// A view whose name happens to share the prefix must not join the union:
	// real BigQuery wildcard tables only span native (STORAGE) tables.
	viewBody, _ := json.Marshal(map[string]any{
		"tableReference": map[string]any{"tableId": "wcv_events_viewshard"},
		"view":           map[string]any{"query": "SELECT 999 AS id, 'view' AS label"},
	})
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(string(viewBody)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("create view: expected 200, got %d: %s", res.Code, res.Body.String())
	}

	out := runQueryAndFetchResults(t, s, "SELECT id FROM `analytics.wcv_events_*`")
	rows, _ := out["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected the view to be excluded from the wildcard union, got %d rows: %+v", len(rows), out)
	}
}

func TestWildcardTableWithNoMatchesFailsExplicitly(t *testing.T) {
	s := newTestServer()
	body, _ := json.Marshal(map[string]any{"configuration": map[string]any{"query": map[string]any{"query": "SELECT * FROM `analytics.does_not_exist_prefix_*`"}}})
	jobOut := runJobAndFetch(t, s, string(body))
	status := jobOut["status"].(map[string]any)
	if status["errorResult"] == nil {
		t.Fatalf("expected an explicit error for a wildcard with no matching tables")
	}
}
