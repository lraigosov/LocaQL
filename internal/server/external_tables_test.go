package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func createDatasetForExternalTest(t *testing.T, s *Server, datasetID string) {
	t.Helper()
	body := `{"datasetReference":{"datasetId":"` + datasetID + `"}}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 creating dataset %s, got %d: %s", datasetID, res.Code, res.Body.String())
	}
}

func insertExternalTable(s *Server, datasetID, tableID string, sourceURIs []string, sourceFormat string, extra map[string]any) *httptest.ResponseRecorder {
	body := map[string]any{
		"tableReference": map[string]any{"tableId": tableID},
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "event_id", "type": "INT64"},
			map[string]any{"name": "event_name", "type": "STRING"},
		}},
		"externalDataConfiguration": mergeMaps(map[string]any{
			"sourceUris":   sourceURIs,
			"sourceFormat": sourceFormat,
		}, extra),
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/"+datasetID+"/tables", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	return res
}

func mergeMaps(base map[string]any, extra map[string]any) map[string]any {
	for k, v := range extra {
		base[k] = v
	}
	return base
}

func TestExternalTableInsertRequiresSourceUris(t *testing.T) {
	s := newTestServer()
	createDatasetForExternalTest(t, s, "ext_ds")

	res := insertExternalTable(s, "ext_ds", "missing_uris", nil, "CSV", nil)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing sourceUris, got %d: %s", res.Code, res.Body.String())
	}
}

func TestExternalTableInsertRequiresSourceFormat(t *testing.T) {
	s := newTestServer()
	createDatasetForExternalTest(t, s, "ext_ds")

	res := insertExternalTable(s, "ext_ds", "missing_format", []string{"/tmp/whatever.csv"}, "", nil)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing sourceFormat, got %d: %s", res.Code, res.Body.String())
	}
}

func TestExternalTableInsertRequiresSchema(t *testing.T) {
	s := newTestServer()
	createDatasetForExternalTest(t, s, "ext_ds")

	body := map[string]any{
		"tableReference": map[string]any{"tableId": "no_schema"},
		"externalDataConfiguration": map[string]any{
			"sourceUris":   []string{"/tmp/whatever.csv"},
			"sourceFormat": "CSV",
		},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/ext_ds/tables", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing schema, got %d: %s", res.Code, res.Body.String())
	}
}

func TestExternalTableInsertRendersTypeAndConfig(t *testing.T) {
	s := newTestServer()
	createDatasetForExternalTest(t, s, "ext_ds")
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(path, []byte("event_id,event_name\n1,login\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	res := insertExternalTable(s, "ext_ds", "events_csv", []string{path}, "CSV", map[string]any{"fieldDelimiter": ",", "skipLeadingRows": 1})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", res.Code, res.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode insert response: %v", err)
	}
	if out["type"] != "EXTERNAL" {
		t.Fatalf("expected type EXTERNAL, got %v", out["type"])
	}
	ext, ok := out["externalDataConfiguration"].(map[string]any)
	if !ok {
		t.Fatalf("expected externalDataConfiguration in response, got %v", out)
	}
	if ext["sourceFormat"] != "CSV" {
		t.Fatalf("expected sourceFormat CSV, got %v", ext["sourceFormat"])
	}
	if ext["skipLeadingRows"] != float64(1) {
		t.Fatalf("expected skipLeadingRows 1, got %v", ext["skipLeadingRows"])
	}
}

// TestExternalTableQueryReflectsLiveFileContents proves external tables are
// read fresh on every query rather than materialized at creation time: the
// same query returns different rows after the underlying CSV file changes,
// with no table update/reload step in between.
func TestExternalTableQueryReflectsLiveFileContents(t *testing.T) {
	s := newTestServer()
	createDatasetForExternalTest(t, s, "ext_ds")
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(path, []byte("1,login\n2,logout\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	insertRes := insertExternalTable(s, "ext_ds", "events_csv", []string{path}, "CSV", nil)
	if insertRes.Code != http.StatusOK {
		t.Fatalf("expected 200 creating external table, got %d: %s", insertRes.Code, insertRes.Body.String())
	}

	runQuery := func() []any {
		body := `{"query":"SELECT * FROM ext_ds.events_csv"}`
		req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		s.Handler().ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			t.Fatalf("expected 200 querying external table, got %d: %s", res.Code, res.Body.String())
		}
		var out map[string]any
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			t.Fatalf("decode query response: %v", err)
		}
		rows, _ := out["rows"].([]any)
		return rows
	}

	firstRows := runQuery()
	if len(firstRows) != 2 {
		t.Fatalf("expected 2 rows before file update, got %d", len(firstRows))
	}

	if err := os.WriteFile(path, []byte("1,login\n2,logout\n3,purchase\n"), 0o600); err != nil {
		t.Fatalf("rewrite fixture: %v", err)
	}

	secondRows := runQuery()
	if len(secondRows) != 3 {
		t.Fatalf("expected 3 rows after file update (external tables read live), got %d", len(secondRows))
	}
}

func TestExternalTableTabledataListReadsFile(t *testing.T) {
	s := newTestServer()
	createDatasetForExternalTest(t, s, "ext_ds")
	dir := t.TempDir()
	path := filepath.Join(dir, "events.ndjson")
	content := "{\"event_id\":1,\"event_name\":\"page_view\"}\n{\"event_id\":2,\"event_name\":\"checkout\"}\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	insertRes := insertExternalTable(s, "ext_ds", "events_ndjson", []string{path}, "NEWLINE_DELIMITED_JSON", nil)
	if insertRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", insertRes.Code, insertRes.Body.String())
	}

	dataReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/tabledata/ext_ds/events_ndjson/data", nil)
	dataRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(dataRes, dataReq)
	if dataRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", dataRes.Code, dataRes.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(dataRes.Body).Decode(&out); err != nil {
		t.Fatalf("decode tabledata response: %v", err)
	}
	if out["totalRows"] != "2" {
		t.Fatalf("expected totalRows 2, got %v", out["totalRows"])
	}
}

// TestExternalTableUnreadableSourceFailsQueryExplicitly proves that a broken
// external table (missing file) surfaces as an explicit query error rather
// than silently falling through to the generic simulated-preview fallback,
// which would otherwise mask the failure as if the table didn't exist.
func TestExternalTableUnreadableSourceFailsQueryExplicitly(t *testing.T) {
	s := newTestServer()
	createDatasetForExternalTest(t, s, "ext_ds")

	insertRes := insertExternalTable(s, "ext_ds", "broken", []string{"/nonexistent/path/does-not-exist.csv"}, "CSV", nil)
	if insertRes.Code != http.StatusOK {
		t.Fatalf("expected 200 creating external table (file existence isn't checked at insert time), got %d: %s", insertRes.Code, insertRes.Body.String())
	}

	body := `{"query":"SELECT * FROM ext_ds.broken"}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 querying an external table whose file is missing, got %d: %s", res.Code, res.Body.String())
	}
}

func TestExternalTableInformationSchemaTablesMarksTypeExternal(t *testing.T) {
	s := newTestServer()
	createDatasetForExternalTest(t, s, "ext_ds")
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(path, []byte("1,login\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if res := insertExternalTable(s, "ext_ds", "events_csv", []string{path}, "CSV", nil); res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", res.Code, res.Body.String())
	}

	body := `{"query":"SELECT * FROM ext_ds.INFORMATION_SCHEMA.TABLES"}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", res.Code, res.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	rows, _ := out["rows"].([]any)
	found := false
	for _, raw := range rows {
		cells := raw.(map[string]any)["f"].([]any)
		tableName := cells[2].(map[string]any)["v"]
		tableType := cells[3].(map[string]any)["v"]
		if tableName == "events_csv" {
			found = true
			if tableType != "EXTERNAL" {
				t.Fatalf("expected table_type EXTERNAL for events_csv, got %v", tableType)
			}
		}
	}
	if !found {
		t.Fatalf("expected events_csv row in INFORMATION_SCHEMA.TABLES, got %v", rows)
	}
}

func TestExternalTableCopyJobMaterializesIntoDestination(t *testing.T) {
	s := newTestServer()
	createDatasetForExternalTest(t, s, "ext_ds")
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(path, []byte("1,login\n2,logout\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if res := insertExternalTable(s, "ext_ds", "events_csv", []string{path}, "CSV", nil); res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", res.Code, res.Body.String())
	}

	copyBody := map[string]any{
		"configuration": map[string]any{
			"copy": map[string]any{
				"sourceTable":      map[string]any{"projectId": "p1", "datasetId": "ext_ds", "tableId": "events_csv"},
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": "ext_ds", "tableId": "events_copy"},
			},
		},
	}
	raw, _ := json.Marshal(copyBody)
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", res.Code, res.Body.String())
	}

	time.Sleep(220 * time.Millisecond)

	dataReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/tabledata/ext_ds/events_copy/data", nil)
	dataRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(dataRes, dataReq)
	if dataRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", dataRes.Code, dataRes.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(dataRes.Body).Decode(&out); err != nil {
		t.Fatalf("decode tabledata response: %v", err)
	}
	if out["totalRows"] != "2" {
		t.Fatalf("expected 2 rows copied from external source, got %v", out["totalRows"])
	}
}
