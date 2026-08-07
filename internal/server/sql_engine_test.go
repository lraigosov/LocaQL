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

func TestReferencedTablesUsesCompleteAST(t *testing.T) {
	query := `
		WITH left_rows AS (
			SELECT customer_id FROM p1.analytics.engine_customers
		), right_rows AS (
			SELECT customer_id FROM ` + "`p1.analytics.engine_purchases`" + `
		)
		SELECT 'FROM p1.analytics.not_a_table' AS literal_value
		FROM left_rows l, right_rows r
		WHERE l.customer_id = r.customer_id`

	refs, err := referencedTablesFromAST(query, "p1")
	if err != nil {
		t.Fatalf("parse table references from AST: %v", err)
	}
	want := map[datasetTableRef]bool{
		{datasetID: "analytics", tableID: "engine_customers"}: true,
		{datasetID: "analytics", tableID: "engine_purchases"}: true,
	}
	if len(refs) != len(want) {
		t.Fatalf("expected exactly %d physical table references, got %d: %#v", len(want), len(refs), refs)
	}
	for _, ref := range refs {
		if !want[ref] {
			t.Fatalf("unexpected AST table reference: %#v", ref)
		}
	}
}

func TestRealSQLEngineCommaJoinMaterializesEveryTable(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "engine_comma_left",
		[]map[string]any{{"name": "id", "type": "INT64"}, {"name": "left_value", "type": "STRING"}},
		"id,left_value\n1,left-one\n2,left-two\n")
	loadCSVTable(t, s, "analytics", "engine_comma_right",
		[]map[string]any{{"name": "id", "type": "INT64"}, {"name": "right_value", "type": "STRING"}},
		"id,right_value\n1,right-one\n3,right-three\n")

	code, out := syncQuery(t, s, `
		SELECT l.left_value, r.right_value
		FROM p1.analytics.engine_comma_left AS l, p1.analytics.engine_comma_right AS r
		WHERE l.id = r.id`)
	if code != http.StatusOK {
		t.Fatalf("expected 200 for comma join, got %d: %v", code, out)
	}
	rows, _ := out["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected one matching comma-join row, got %d: %v", len(rows), rows)
	}
	if got := cellsOf(t, out, 0); got[0] != "left-one" || got[1] != "right-one" {
		t.Fatalf("expected [left-one right-one], got %v", got)
	}
}

func TestQueryProcessedBytesMeasureSourcesNotResult(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "engine_scan_bytes",
		[]map[string]any{{"name": "id", "type": "INT64"}, {"name": "payload", "type": "STRING"}},
		"id,payload\n1,large-payload-one\n2,large-payload-two\n3,large-payload-three\n")

	result, err := s.executeQueryStatement("p1", "", "SELECT COUNT(*) AS n FROM p1.analytics.engine_scan_bytes", "", "", nil)
	if err != nil {
		t.Fatalf("execute aggregate query: %v", err)
	}
	table, ok, _ := s.tables.get("p1", "analytics", "engine_scan_bytes")
	if !ok {
		t.Fatal("expected source table in catalog")
	}
	want := estimateRowsByteSize(table.Rows)
	if result.processedBytes != want {
		t.Fatalf("expected %d bytes from all source rows, got %d", want, result.processedBytes)
	}
	if result.processedBytes == estimateRowsByteSize(result.rows) {
		t.Fatalf("processed bytes must not be derived from the aggregate result row: %v", result.rows)
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
