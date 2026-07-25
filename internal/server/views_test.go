package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func insertView(s *Server, datasetID, tableID, query string, materialized bool) *httptest.ResponseRecorder {
	key := "view"
	if materialized {
		key = "materializedView"
	}
	body := map[string]any{
		"tableReference": map[string]any{"tableId": tableID},
		key:              map[string]any{"query": query},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/"+datasetID+"/tables", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	return res
}

func TestViewInsertDerivesSchemaAndType(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "view_src_orders",
		[]map[string]any{
			{"name": "region", "type": "STRING"},
			{"name": "amount", "type": "FLOAT64"},
		},
		"region,amount\nUS,10\nUS,20\nEU,5\n")

	res := insertView(s, "analytics", "view_high_value",
		"SELECT region, amount FROM p1.analytics.view_src_orders WHERE amount > 8", false)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 creating view, got %d: %s", res.Code, res.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode view resource: %v", err)
	}
	if out["type"] != "VIEW" {
		t.Fatalf("expected type VIEW, got %v", out["type"])
	}
	view, ok := out["view"].(map[string]any)
	if !ok || view["query"] == "" {
		t.Fatalf("expected view.query in response, got %v", out["view"])
	}
	fields := out["schema"].(map[string]any)["fields"].([]any)
	if len(fields) != 2 {
		t.Fatalf("expected schema derived from the view query (2 columns), got %v", fields)
	}
	if fields[0].(map[string]any)["name"] != "region" || fields[1].(map[string]any)["name"] != "amount" {
		t.Fatalf("unexpected derived schema field names: %v", fields)
	}
}

func TestViewQueryReflectsLiveUnderlyingTableData(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "view_src_events",
		[]map[string]any{
			{"name": "event_name", "type": "STRING"},
		},
		"event_name\nalpha\n")

	res := insertView(s, "analytics", "view_all_events", "SELECT event_name FROM p1.analytics.view_src_events", false)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 creating view, got %d: %s", res.Code, res.Body.String())
	}

	code, out := syncQuery(t, s, "SELECT * FROM p1.analytics.view_all_events")
	if code != http.StatusOK {
		t.Fatalf("expected 200 querying view, got %d: %v", code, out)
	}
	if rows, _ := out["rows"].([]any); len(rows) != 1 {
		t.Fatalf("expected 1 row before append, got %v", rows)
	}

	// Append a second row to the underlying table with WRITE_APPEND and
	// re-query the view: it must reflect the new data, proving the view is
	// re-executed live rather than cached at creation time.
	body, _ := json.Marshal(map[string]any{
		"configuration": map[string]any{
			"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": "analytics", "tableId": "view_src_events"},
				"schema":           map[string]any{"fields": []any{map[string]any{"name": "event_name", "type": "STRING"}}},
				"sourceUris":       []any{writeTempCSV(t, "event_name\nbeta\n")},
				"sourceFormat":     "CSV",
				"skipLeadingRows":  1,
				"writeDisposition": "WRITE_APPEND",
			},
		},
	})
	jobOut := runJobAndFetch(t, s, string(body))
	if status := jobOut["status"].(map[string]any); status["errorResult"] != nil {
		t.Fatalf("unexpected append load error: %v", status["errorResult"])
	}

	code, out = syncQuery(t, s, "SELECT * FROM p1.analytics.view_all_events")
	if code != http.StatusOK {
		t.Fatalf("expected 200 re-querying view, got %d: %v", code, out)
	}
	rows, _ := out["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("expected view to reflect the appended row (2 total), got %d: %v", len(rows), rows)
	}
}

func TestViewChainedOnAnotherViewResolvesRecursively(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "view_chain_base",
		[]map[string]any{
			{"name": "n", "type": "INT64"},
		},
		"n\n1\n2\n3\n")

	if res := insertView(s, "analytics", "view_chain_mid", "SELECT n FROM p1.analytics.view_chain_base WHERE n > 1", false); res.Code != http.StatusOK {
		t.Fatalf("expected 200 creating mid view, got %d: %s", res.Code, res.Body.String())
	}
	if res := insertView(s, "analytics", "view_chain_top", "SELECT n FROM p1.analytics.view_chain_mid WHERE n < 3", false); res.Code != http.StatusOK {
		t.Fatalf("expected 200 creating top view (view-on-view), got %d: %s", res.Code, res.Body.String())
	}

	code, out := syncQuery(t, s, "SELECT * FROM p1.analytics.view_chain_top")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	rows, _ := out["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected view-on-view to resolve to exactly [2], got %v", rows)
	}
	if got := cellsOf(t, out, 0); got[0] != "2" {
		t.Fatalf("expected row [2], got %v", got)
	}
}

func TestMaterializedViewInsertAndInformationSchema(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "matview_src",
		[]map[string]any{{"name": "n", "type": "INT64"}}, "n\n1\n2\n")

	if res := insertView(s, "analytics", "matview_totals", "SELECT COUNT(*) AS total FROM p1.analytics.matview_src", true); res.Code != http.StatusOK {
		t.Fatalf("expected 200 creating materialized view, got %d: %s", res.Code, res.Body.String())
	}
	if res := insertView(s, "analytics", "regular_view", "SELECT n FROM p1.analytics.matview_src", false); res.Code != http.StatusOK {
		t.Fatalf("expected 200 creating regular view, got %d: %s", res.Code, res.Body.String())
	}

	code, out := syncQuery(t, s, "SELECT * FROM p1.analytics.INFORMATION_SCHEMA.MATERIALIZED_VIEWS")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	rows, _ := out["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 materialized view listed, got %v", rows)
	}

	code, out = syncQuery(t, s, "SELECT * FROM p1.analytics.INFORMATION_SCHEMA.VIEWS")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	rows, _ = out["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected INFORMATION_SCHEMA.VIEWS to list only the regular view, not the materialized one, got %v", rows)
	}

	code, out = syncQuery(t, s, "SELECT * FROM p1.analytics.INFORMATION_SCHEMA.TABLES")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	rows, _ = out["rows"].([]any)
	// INFORMATION_SCHEMA builders don't support column projection (a
	// pre-existing limitation, unrelated to views): the full fixed schema
	// (table_catalog, table_schema, table_name, table_type) always comes
	// back regardless of the SELECT list, so index by position instead.
	found := map[string]string{}
	for i := range rows {
		cells := cellsOf(t, out, i)
		found[cells[2].(string)] = cells[3].(string)
	}
	if found["matview_totals"] != "MATERIALIZED VIEW" {
		t.Fatalf("expected matview_totals table_type MATERIALIZED VIEW, got %v", found)
	}
	if found["regular_view"] != "VIEW" {
		t.Fatalf("expected regular_view table_type VIEW, got %v", found)
	}
}

func TestViewInsertRejectsInvalidQueryExplicitly(t *testing.T) {
	s := newTestServer()
	res := insertView(s, "analytics", "view_bad_ref", "SELECT * FROM p1.analytics.table_that_does_not_exist", false)
	if res.Code == http.StatusOK {
		t.Fatalf("expected an explicit error creating a view over a missing table, got 200: %s", res.Body.String())
	}
}

func TestViewInsertRejectsSelfReference(t *testing.T) {
	s := newTestServer()
	res := insertView(s, "analytics", "view_self", "SELECT * FROM p1.analytics.view_self", false)
	if res.Code == http.StatusOK {
		t.Fatalf("expected an explicit error creating a self-referencing view (it doesn't exist yet), got 200: %s", res.Body.String())
	}
}

func TestViewInsertRejectsBothViewAndMaterializedView(t *testing.T) {
	s := newTestServer()
	body := map[string]any{
		"tableReference":   map[string]any{"tableId": "view_both"},
		"view":             map[string]any{"query": "SELECT 1"},
		"materializedView": map[string]any{"query": "SELECT 1"},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code == http.StatusOK {
		t.Fatalf("expected an explicit error when both view and materializedView are set, got 200")
	}
}

func writeTempCSV(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "append.csv")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp csv: %v", err)
	}
	return path
}
