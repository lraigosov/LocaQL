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

func loadCSVTable(t *testing.T, s *Server, datasetID, tableID string, fields []map[string]any, csvContent string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, tableID+".csv")
	if err := os.WriteFile(path, []byte(csvContent), 0o600); err != nil {
		t.Fatalf("write csv fixture for %s: %v", tableID, err)
	}
	body, err := json.Marshal(map[string]any{
		"configuration": map[string]any{
			"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": datasetID, "tableId": tableID},
				"schema":           map[string]any{"fields": fields},
				"sourceUris":       []any{path},
				"sourceFormat":     "CSV",
				"skipLeadingRows":  1,
				"writeDisposition": "WRITE_TRUNCATE",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal load body: %v", err)
	}
	jobOut := runJobAndFetch(t, s, string(body))
	status := jobOut["status"].(map[string]any)
	if status["errorResult"] != nil {
		t.Fatalf("unexpected load error for %s: %v", tableID, status["errorResult"])
	}
}

func syncQuery(t *testing.T, s *Server, query string) (int, map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"query": query})
	if err != nil {
		t.Fatalf("marshal query body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	return res.Code, out
}

func cellsOf(t *testing.T, out map[string]any, rowIdx int) []any {
	t.Helper()
	rows, _ := out["rows"].([]any)
	if rowIdx >= len(rows) {
		t.Fatalf("row index %d out of range (have %d rows)", rowIdx, len(rows))
	}
	f := rows[rowIdx].(map[string]any)["f"].([]any)
	vals := make([]any, len(f))
	for i, c := range f {
		vals[i] = c.(map[string]any)["v"]
	}
	return vals
}

func intFields() []map[string]any {
	return []map[string]any{
		{"name": "event_id", "type": "INT64"},
		{"name": "event_name", "type": "STRING"},
		{"name": "amount", "type": "FLOAT64"},
	}
}

func TestRealSQLEngineWhereProjectionAndOrderBy(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "engine_events", intFields(),
		"event_id,event_name,amount\n1,alpha,10.5\n2,beta,20.25\n3,gamma,5\n")

	code, out := syncQuery(t, s, "SELECT event_name, amount FROM p1.analytics.engine_events WHERE amount > 6 ORDER BY amount DESC")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	rows, _ := out["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("expected WHERE to filter down to 2 rows, got %d: %v", len(rows), rows)
	}
	if got := cellsOf(t, out, 0); got[0] != "beta" || got[1] != "20.25" {
		t.Fatalf("expected first row [beta 20.25] after ORDER BY amount DESC, got %v", got)
	}
	if got := cellsOf(t, out, 1); got[0] != "alpha" || got[1] != "10.5" {
		t.Fatalf("expected second row [alpha 10.5], got %v", got)
	}
}

func TestRealSQLEngineGroupByAggregate(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "engine_orders",
		[]map[string]any{
			{"name": "region", "type": "STRING"},
			{"name": "amount", "type": "FLOAT64"},
		},
		"region,amount\nUS,10\nUS,20\nEU,5\n")

	code, out := syncQuery(t, s, "SELECT region, COUNT(*) AS n, SUM(amount) AS total FROM p1.analytics.engine_orders GROUP BY region ORDER BY region")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if got := cellsOf(t, out, 0); got[0] != "EU" || got[1] != "1" || got[2] != "5" {
		t.Fatalf("expected [EU 1 5], got %v", got)
	}
	if got := cellsOf(t, out, 1); got[0] != "US" || got[1] != "2" || got[2] != "30" {
		t.Fatalf("expected [US 2 30], got %v", got)
	}
}

func TestRealSQLEngineJoinAcrossTables(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "engine_customers",
		[]map[string]any{
			{"name": "customer_id", "type": "INT64"},
			{"name": "customer_name", "type": "STRING"},
		},
		"customer_id,customer_name\n1,alice\n2,bob\n")
	loadCSVTable(t, s, "analytics", "engine_purchases",
		[]map[string]any{
			{"name": "customer_id", "type": "INT64"},
			{"name": "item", "type": "STRING"},
		},
		"customer_id,item\n1,widget\n2,gadget\n")

	code, out := syncQuery(t, s, "SELECT c.customer_name, p.item FROM p1.analytics.engine_customers c JOIN p1.analytics.engine_purchases p ON c.customer_id = p.customer_id ORDER BY c.customer_name")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if got := cellsOf(t, out, 0); got[0] != "alice" || got[1] != "widget" {
		t.Fatalf("expected [alice widget], got %v", got)
	}
	if got := cellsOf(t, out, 1); got[0] != "bob" || got[1] != "gadget" {
		t.Fatalf("expected [bob gadget], got %v", got)
	}
}

func TestRealSQLEngineLimit(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "engine_limit_rows",
		[]map[string]any{{"name": "n", "type": "INT64"}},
		"n\n1\n2\n3\n4\n")

	code, out := syncQuery(t, s, "SELECT n FROM p1.analytics.engine_limit_rows ORDER BY n LIMIT 2")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	rows, _ := out["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("expected LIMIT 2 to cap the result at 2 rows, got %d: %v", len(rows), rows)
	}
}

func TestRealSQLEngineInvalidQueryFailsExplicitlyInsteadOfFabricatingRows(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "engine_bad_query",
		[]map[string]any{{"name": "n", "type": "INT64"}}, "n\n1\n")

	code, out := syncQuery(t, s, "SELECT n FROM p1.analytics.engine_bad_query WHERE THIS IS NOT VALID SQL !!!")
	if code == http.StatusOK {
		t.Fatalf("expected an explicit error for invalid SQL, got 200: %v", out)
	}
	msg, _ := out["error"].(map[string]any)["message"].(string)
	if strings.Contains(msg, "Simulated query result row") {
		t.Fatalf("expected a real parser error, not the old fabricated fallback: %v", msg)
	}
}

func TestRealSQLEngineMissingTableFailsExplicitly(t *testing.T) {
	s := newTestServer()
	code, out := syncQuery(t, s, "SELECT * FROM p1.analytics.table_that_does_not_exist")
	if code == http.StatusOK {
		t.Fatalf("expected an explicit error for a missing table, got 200: %v", out)
	}
	if out["error"] == nil {
		t.Fatalf("expected an error object in the response, got %v", out)
	}
}
