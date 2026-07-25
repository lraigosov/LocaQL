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

func loadNDJSONTable(t *testing.T, s *Server, datasetID, tableID string, fields []map[string]any, ndjsonContent string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, tableID+".ndjson")
	if err := os.WriteFile(path, []byte(ndjsonContent), 0o600); err != nil {
		t.Fatalf("write ndjson fixture for %s: %v", tableID, err)
	}
	body, err := json.Marshal(map[string]any{
		"configuration": map[string]any{
			"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": datasetID, "tableId": tableID},
				"schema":           map[string]any{"fields": fields},
				"sourceUris":       []any{path},
				"sourceFormat":     "NEWLINE_DELIMITED_JSON",
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

func tableDataRows(t *testing.T, s *Server, datasetID, tableID string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/tabledata/"+datasetID+"/"+tableID+"/data", nil)
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 fetching tabledata, got %d: %s", res.Code, res.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode tabledata: %v", err)
	}
	return out
}

func recordCellsAt(t *testing.T, out map[string]any, rowIdx, colIdx int) []any {
	t.Helper()
	rows := out["rows"].([]any)
	row := rows[rowIdx].(map[string]any)["f"].([]any)
	cell := row[colIdx].(map[string]any)["v"]
	nested, ok := cell.(map[string]any)
	if !ok {
		t.Fatalf("expected a nested RECORD cell ({\"f\": [...]}) at row %d col %d, got %#v", rowIdx, colIdx, cell)
	}
	return nested["f"].([]any)
}

func recordFieldValue(t *testing.T, cells []any, idx int) any {
	t.Helper()
	return cells[idx].(map[string]any)["v"]
}

func TestNestedRecordSchemaInsertAndTableResourceRendersModeAndFields(t *testing.T) {
	s := newTestServer()
	body := map[string]any{
		"tableReference": map[string]any{"tableId": "nested_users"},
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "user_id", "type": "INT64"},
			map[string]any{"name": "address", "type": "RECORD", "fields": []any{
				map[string]any{"name": "city", "type": "STRING"},
				map[string]any{"name": "zip", "type": "STRING"},
			}},
		}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", res.Code, res.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	fields := out["schema"].(map[string]any)["fields"].([]any)
	addr := fields[1].(map[string]any)
	if addr["type"] != "RECORD" {
		t.Fatalf("expected address field type RECORD, got %v", addr["type"])
	}
	if addr["mode"] != "NULLABLE" {
		t.Fatalf("expected default mode NULLABLE, got %v", addr["mode"])
	}
	nestedFields := addr["fields"].([]any)
	if len(nestedFields) != 2 {
		t.Fatalf("expected 2 nested fields, got %v", nestedFields)
	}
	if nestedFields[0].(map[string]any)["name"] != "city" {
		t.Fatalf("expected first nested field 'city', got %v", nestedFields[0])
	}
}

func TestNestedRecordLoadQueryAndTabledataRoundTrip(t *testing.T) {
	s := newTestServer()
	fields := []map[string]any{
		{"name": "user_id", "type": "INT64"},
		{"name": "address", "type": "RECORD", "fields": []any{
			map[string]any{"name": "city", "type": "STRING"},
			map[string]any{"name": "zip", "type": "STRING"},
		}},
	}
	loadNDJSONTable(t, s, "analytics", "nested_load_users", fields,
		`{"user_id":1,"address":{"city":"NYC","zip":"10001"}}`+"\n"+
			`{"user_id":2,"address":{"city":"LA","zip":"90001"}}`+"\n")

	out := tableDataRows(t, s, "analytics", "nested_load_users")
	if out["totalRows"] != "2" {
		t.Fatalf("expected 2 rows, got %v", out["totalRows"])
	}
	cells := recordCellsAt(t, out, 0, 1)
	if recordFieldValue(t, cells, 0) != "NYC" || recordFieldValue(t, cells, 1) != "10001" {
		t.Fatalf("expected nested [NYC, 10001], got %v", cells)
	}

	code, qout := syncQuery(t, s, "SELECT user_id, address FROM p1.analytics.nested_load_users WHERE user_id = 2")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, qout)
	}
	qcells := recordCellsAt(t, qout, 0, 1)
	if recordFieldValue(t, qcells, 0) != "LA" {
		t.Fatalf("expected query result nested address.city = LA, got %v", qcells)
	}
}

func TestNestedRecordExtractToNDJSONRoundTrip(t *testing.T) {
	s := newTestServer()
	fields := []map[string]any{
		{"name": "user_id", "type": "INT64"},
		{"name": "address", "type": "RECORD", "fields": []any{
			map[string]any{"name": "city", "type": "STRING"},
		}},
	}
	loadNDJSONTable(t, s, "analytics", "nested_extract_users", fields,
		`{"user_id":1,"address":{"city":"NYC"}}`+"\n")

	dir := t.TempDir()
	outPath := filepath.Join(dir, "out.ndjson")
	body, _ := json.Marshal(map[string]any{
		"configuration": map[string]any{
			"extract": map[string]any{
				"sourceTable":       map[string]any{"projectId": "p1", "datasetId": "analytics", "tableId": "nested_extract_users"},
				"destinationUris":   []any{outPath},
				"destinationFormat": "NEWLINE_DELIMITED_JSON",
			},
		},
	})
	jobOut := runJobAndFetch(t, s, string(body))
	if status := jobOut["status"].(map[string]any); status["errorResult"] != nil {
		t.Fatalf("unexpected extract error: %v", status["errorResult"])
	}

	content, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(content, &decoded); err != nil {
		t.Fatalf("decode extracted NDJSON: %v (content: %s)", err, content)
	}
	addr, ok := decoded["address"].(map[string]any)
	if !ok {
		t.Fatalf("expected address to be a real nested JSON object in extracted NDJSON, got %#v", decoded["address"])
	}
	if addr["city"] != "NYC" {
		t.Fatalf("expected city=NYC, got %v", addr)
	}
}

func TestRepeatedScalarLoadQueryAndTabledataRoundTrip(t *testing.T) {
	s := newTestServer()
	fields := []map[string]any{
		{"name": "user_id", "type": "INT64"},
		{"name": "tags", "type": "STRING", "mode": "REPEATED"},
	}
	loadNDJSONTable(t, s, "analytics", "repeated_tags_users", fields,
		`{"user_id":1,"tags":["a","b","c"]}`+"\n")

	out := tableDataRows(t, s, "analytics", "repeated_tags_users")
	rows := out["rows"].([]any)
	f := rows[0].(map[string]any)["f"].([]any)
	tagsCell := f[1].(map[string]any)["v"]
	arr, ok := tagsCell.([]any)
	if !ok || len(arr) != 3 {
		t.Fatalf("expected a 3-element REPEATED array, got %#v", tagsCell)
	}
	if arr[0].(map[string]any)["v"] != "a" || arr[2].(map[string]any)["v"] != "c" {
		t.Fatalf("expected [a,b,c], got %v", arr)
	}

	code, qout := syncQuery(t, s, "SELECT tags FROM p1.analytics.repeated_tags_users")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, qout)
	}
	qrows := qout["rows"].([]any)
	qf := qrows[0].(map[string]any)["f"].([]any)
	qarr, ok := qf[0].(map[string]any)["v"].([]any)
	if !ok || len(qarr) != 3 {
		t.Fatalf("expected query result REPEATED array of 3, got %#v", qf[0])
	}
}

func TestRepeatedRecordArrayOfStructRoundTrip(t *testing.T) {
	s := newTestServer()
	fields := []map[string]any{
		{"name": "order_id", "type": "INT64"},
		{"name": "items", "type": "RECORD", "mode": "REPEATED", "fields": []any{
			map[string]any{"name": "sku", "type": "STRING"},
			map[string]any{"name": "qty", "type": "INT64"},
		}},
	}
	loadNDJSONTable(t, s, "analytics", "repeated_struct_orders", fields,
		`{"order_id":1,"items":[{"sku":"A1","qty":2},{"sku":"B2","qty":1}]}`+"\n")

	code, qout := syncQuery(t, s, "SELECT items FROM p1.analytics.repeated_struct_orders")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, qout)
	}
	rows := qout["rows"].([]any)
	f := rows[0].(map[string]any)["f"].([]any)
	items, ok := f[0].(map[string]any)["v"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected ARRAY<STRUCT> of 2 items, got %#v", f[0])
	}
	first := items[0].(map[string]any)["v"].(map[string]any)["f"].([]any)
	if recordFieldValue(t, first, 0) != "A1" || recordFieldValue(t, first, 1) != "2" {
		t.Fatalf("expected first item [A1, 2], got %v", first)
	}
}

func TestCSVLoadRejectsNestedSchemaFieldsExplicitly(t *testing.T) {
	s := newTestServer()
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(path, []byte("a\n1\n"), 0o600); err != nil {
		t.Fatalf("write csv fixture: %v", err)
	}
	body, _ := json.Marshal(map[string]any{
		"configuration": map[string]any{
			"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": "analytics", "tableId": "csv_nested_rejected"},
				"schema": map[string]any{"fields": []any{
					map[string]any{"name": "a", "type": "RECORD", "fields": []any{map[string]any{"name": "x", "type": "STRING"}}},
				}},
				"sourceUris":       []any{path},
				"sourceFormat":     "CSV",
				"skipLeadingRows":  1,
				"writeDisposition": "WRITE_TRUNCATE",
			},
		},
	})
	jobOut := runJobAndFetch(t, s, string(body))
	status := jobOut["status"].(map[string]any)
	if status["errorResult"] == nil {
		t.Fatalf("expected an explicit error loading a RECORD field via CSV, got success")
	}
}

func TestTablesPatchSchemaEvolutionAddsNullableColumn(t *testing.T) {
	s := newTestServer()
	insertBody, _ := json.Marshal(map[string]any{
		"tableReference": map[string]any{"tableId": "evolve_me"},
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "id", "type": "INT64"},
		}},
	})
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(string(insertBody)))
	insertReq.Header.Set("Content-Type", "application/json")
	insertRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertRes, insertReq)
	if insertRes.Code != http.StatusOK {
		t.Fatalf("expected 200 creating table, got %d: %s", insertRes.Code, insertRes.Body.String())
	}

	patchBody, _ := json.Marshal(map[string]any{
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "id", "type": "INT64"},
			map[string]any{"name": "new_col", "type": "STRING"},
		}},
	})
	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/analytics/tables/evolve_me", strings.NewReader(string(patchBody)))
	patchReq.Header.Set("Content-Type", "application/json")
	patchRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchRes, patchReq)
	if patchRes.Code != http.StatusOK {
		t.Fatalf("expected 200 patching schema, got %d: %s", patchRes.Code, patchRes.Body.String())
	}
	var out map[string]any
	json.NewDecoder(patchRes.Body).Decode(&out)
	fields := out["schema"].(map[string]any)["fields"].([]any)
	if len(fields) != 2 || fields[1].(map[string]any)["name"] != "new_col" {
		t.Fatalf("expected 2 fields with new_col appended, got %v", fields)
	}
}

func TestTablesPatchSchemaEvolutionRejectsRemovingColumn(t *testing.T) {
	s := newTestServer()
	insertBody, _ := json.Marshal(map[string]any{
		"tableReference": map[string]any{"tableId": "evolve_no_remove"},
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "a", "type": "INT64"},
			map[string]any{"name": "b", "type": "STRING"},
		}},
	})
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(string(insertBody)))
	insertReq.Header.Set("Content-Type", "application/json")
	s.Handler().ServeHTTP(httptest.NewRecorder(), insertReq)

	patchBody, _ := json.Marshal(map[string]any{
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "a", "type": "INT64"},
		}},
	})
	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/analytics/tables/evolve_no_remove", strings.NewReader(string(patchBody)))
	patchReq.Header.Set("Content-Type", "application/json")
	patchRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchRes, patchReq)
	if patchRes.Code == http.StatusOK {
		t.Fatalf("expected an explicit error removing an existing column, got 200")
	}
}

func TestTablesPatchSchemaEvolutionAllowsRelaxingRequiredToNullable(t *testing.T) {
	s := newTestServer()
	insertBody, _ := json.Marshal(map[string]any{
		"tableReference": map[string]any{"tableId": "evolve_relax"},
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "a", "type": "INT64", "mode": "REQUIRED"},
		}},
	})
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(string(insertBody)))
	insertReq.Header.Set("Content-Type", "application/json")
	s.Handler().ServeHTTP(httptest.NewRecorder(), insertReq)

	patchBody, _ := json.Marshal(map[string]any{
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "a", "type": "INT64", "mode": "NULLABLE"},
		}},
	})
	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/analytics/tables/evolve_relax", strings.NewReader(string(patchBody)))
	patchReq.Header.Set("Content-Type", "application/json")
	patchRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchRes, patchReq)
	if patchRes.Code != http.StatusOK {
		t.Fatalf("expected 200 relaxing REQUIRED to NULLABLE, got %d: %s", patchRes.Code, patchRes.Body.String())
	}
}

func TestTablesPatchSchemaEvolutionRejectsTighteningNullableToRequired(t *testing.T) {
	s := newTestServer()
	insertBody, _ := json.Marshal(map[string]any{
		"tableReference": map[string]any{"tableId": "evolve_no_tighten"},
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "a", "type": "INT64"},
		}},
	})
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(string(insertBody)))
	insertReq.Header.Set("Content-Type", "application/json")
	s.Handler().ServeHTTP(httptest.NewRecorder(), insertReq)

	patchBody, _ := json.Marshal(map[string]any{
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "a", "type": "INT64", "mode": "REQUIRED"},
		}},
	})
	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/analytics/tables/evolve_no_tighten", strings.NewReader(string(patchBody)))
	patchReq.Header.Set("Content-Type", "application/json")
	patchRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchRes, patchReq)
	if patchRes.Code == http.StatusOK {
		t.Fatalf("expected an explicit error tightening NULLABLE to REQUIRED, got 200")
	}
}
