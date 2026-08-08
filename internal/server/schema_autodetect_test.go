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

func schemaFieldByName(t *testing.T, schema []tableField, name string) tableField {
	t.Helper()
	for _, f := range schema {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("field %q not found in detected schema %+v", name, schema)
	return tableField{}
}

func TestDetectSchemaFromNDJSONWidensAndDefaultsNullable(t *testing.T) {
	data := []byte(strings.Join([]string{
		`{"id": 1, "amount": 10, "label": "a", "active": true}`,
		`{"id": 2, "amount": 10.5, "label": "b", "active": false}`,
		`{"id": 3, "amount": 7, "label": null, "active": true}`,
	}, "\n"))

	schema, err := detectSchemaFromNDJSON(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := schemaFieldByName(t, schema, "id"); got.Type != "INT64" || got.Mode != "NULLABLE" {
		t.Fatalf("id: got %+v", got)
	}
	if got := schemaFieldByName(t, schema, "amount"); got.Type != "FLOAT64" {
		t.Fatalf("amount should widen INT64+FLOAT64 to FLOAT64, got %+v", got)
	}
	if got := schemaFieldByName(t, schema, "label"); got.Type != "STRING" {
		t.Fatalf("label: got %+v", got)
	}
	if got := schemaFieldByName(t, schema, "active"); got.Type != "BOOL" {
		t.Fatalf("active: got %+v", got)
	}
}

func TestDetectSchemaFromNDJSONRejectsNestedValues(t *testing.T) {
	_, err := detectSchemaFromNDJSON([]byte(`{"id": 1, "address": {"city": "Bogota"}}`))
	if err == nil {
		t.Fatalf("expected an error for a nested value")
	}
}

func TestDetectSchemaFromNDJSONFailsOnEmptyInput(t *testing.T) {
	if _, err := detectSchemaFromNDJSON([]byte("")); err == nil {
		t.Fatalf("expected an error for no rows")
	}
}

func TestDetectSchemaFromCSVWithHeaderRow(t *testing.T) {
	data := []byte("id,name,active\n1,Alice,true\n2,Bob,false\n3,Carol,true\n")
	schema, headerRows, err := detectSchemaFromCSV(data, ",", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// A caller (external table config, load job config) must fold this into
	// its own skipLeadingRows, or the real row-reading path re-reads this
	// header line as a bogus data row — see autodetectResult's doc comment.
	if headerRows != 1 {
		t.Fatalf("expected the detected header row to be reported, got headerRows=%d", headerRows)
	}
	if len(schema) != 3 {
		t.Fatalf("expected 3 columns, got %d: %+v", len(schema), schema)
	}
	if got := schemaFieldByName(t, schema, "id"); got.Type != "INT64" {
		t.Fatalf("id: got %+v", got)
	}
	if got := schemaFieldByName(t, schema, "name"); got.Type != "STRING" {
		t.Fatalf("name: got %+v", got)
	}
	if got := schemaFieldByName(t, schema, "active"); got.Type != "BOOL" {
		t.Fatalf("active: got %+v", got)
	}
}

func TestDetectSchemaFromCSVWithoutHeaderRowUsesGenericNames(t *testing.T) {
	// Every row, including the first, looks like data (all-numeric first
	// column), so there is no header signal and columns fall back to
	// BigQuery's own generic naming convention.
	data := []byte("1,10.5\n2,20.5\n3,30.5\n")
	schema, _, err := detectSchemaFromCSV(data, ",", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(schema) != 2 || schema[0].Name != "string_field_0" || schema[1].Name != "string_field_1" {
		t.Fatalf("expected generic column names, got %+v", schema)
	}
	if schema[0].Type != "INT64" || schema[1].Type != "FLOAT64" {
		t.Fatalf("expected inferred types, got %+v", schema)
	}
}

func TestDetectSchemaFromCSVHonorsSkipLeadingRows(t *testing.T) {
	data := []byte("garbage preamble line\nid,amount\n1,5\n2,6\n")
	schema, _, err := detectSchemaFromCSV(data, ",", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := schemaFieldByName(t, schema, "id"); got.Type != "INT64" {
		t.Fatalf("id: got %+v", got)
	}
	if got := schemaFieldByName(t, schema, "amount"); got.Type != "INT64" {
		t.Fatalf("amount: got %+v", got)
	}
}

func TestDetectSchemaFromCSVFailsOnEmptyInput(t *testing.T) {
	if _, _, err := detectSchemaFromCSV([]byte(""), ",", 0); err == nil {
		t.Fatalf("expected an error for no rows")
	}
}

func TestDetectSchemaFromAvroReadsEmbeddedSchemaExactly(t *testing.T) {
	original := []tableField{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "amount", Type: "FLOAT64"},
		{Name: "label", Type: "STRING"},
	}
	encoded, err := encodeAvro(original, [][]string{{"1", "1.5", "a"}}, "null")
	if err != nil {
		t.Fatalf("encodeAvro: %v", err)
	}

	detected, err := detectSchemaFromAvro(encoded)
	if err != nil {
		t.Fatalf("detectSchemaFromAvro: %v", err)
	}
	if got := schemaFieldByName(t, detected, "id"); got.Type != "INT64" || got.Mode != "REQUIRED" {
		t.Fatalf("id: got %+v", got)
	}
	if got := schemaFieldByName(t, detected, "amount"); got.Type != "FLOAT64" || got.Mode != "NULLABLE" {
		t.Fatalf("amount: got %+v", got)
	}
	if got := schemaFieldByName(t, detected, "label"); got.Type != "STRING" || got.Mode != "NULLABLE" {
		t.Fatalf("label: got %+v", got)
	}
}

func TestDetectSchemaFromParquetReadsEmbeddedSchemaExactly(t *testing.T) {
	original := []tableField{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "active", Type: "BOOL"},
	}
	encoded, err := encodeParquet(original, [][]string{{"1", "true"}}, nil)
	if err != nil {
		t.Fatalf("encodeParquet: %v", err)
	}

	detected, err := detectSchemaFromParquet(encoded)
	if err != nil {
		t.Fatalf("detectSchemaFromParquet: %v", err)
	}
	if got := schemaFieldByName(t, detected, "id"); got.Type != "INT64" || got.Mode != "REQUIRED" {
		t.Fatalf("id: got %+v", got)
	}
	if got := schemaFieldByName(t, detected, "active"); got.Type != "BOOL" || got.Mode != "NULLABLE" {
		t.Fatalf("active: got %+v", got)
	}
}

func TestExternalTableAutodetectFromCSV(t *testing.T) {
	s := newTestServer()
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(path, []byte("id,label\n1,alpha\n2,beta\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"tableReference": map[string]any{"tableId": "autodetected_ext"},
		"externalDataConfiguration": map[string]any{
			"sourceUris":   []any{path},
			"sourceFormat": "CSV",
			"autodetect":   true,
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", res.Code, res.Body.String())
	}
	var created map[string]any
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	edc, ok := created["externalDataConfiguration"].(map[string]any)
	if !ok || edc["autodetect"] != true {
		t.Fatalf("expected autodetect:true to round-trip, got %+v", created["externalDataConfiguration"])
	}
	schemaOut, ok := created["schema"].(map[string]any)
	if !ok {
		t.Fatalf("expected a detected schema in the create response, got %+v", created)
	}
	fields, _ := schemaOut["fields"].([]any)
	if len(fields) != 2 {
		t.Fatalf("expected 2 detected fields, got %+v", fields)
	}

	data := tableDataRows(t, s, "analytics", "autodetected_ext")
	if data["totalRows"] != "2" {
		t.Fatalf("expected 2 live-read rows, got totalRows=%v", data["totalRows"])
	}
}

func TestExternalTableWithoutSchemaOrAutodetectFailsExplicitly(t *testing.T) {
	s := newTestServer()
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	_ = os.WriteFile(path, []byte("id,label\n1,alpha\n"), 0o600)

	body, _ := json.Marshal(map[string]any{
		"tableReference": map[string]any{"tableId": "no_schema_ext"},
		"externalDataConfiguration": map[string]any{
			"sourceUris":   []any{path},
			"sourceFormat": "CSV",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", res.Code, res.Body.String())
	}
}

func TestLoadJobAutodetectFromNDJSON(t *testing.T) {
	s := newTestServer()
	dir := t.TempDir()
	path := filepath.Join(dir, "rows.ndjson")
	content := `{"id": 1, "score": 9.5}
{"id": 2, "score": 8}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"configuration": map[string]any{
			"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": "analytics", "tableId": "autodetected_load"},
				"sourceUris":       []any{path},
				"sourceFormat":     "NEWLINE_DELIMITED_JSON",
				"autodetect":       true,
				"writeDisposition": "WRITE_TRUNCATE",
			},
		},
	})
	jobOut := runJobAndFetch(t, s, string(body))
	status := jobOut["status"].(map[string]any)
	if status["errorResult"] != nil {
		t.Fatalf("unexpected load error: %v", status["errorResult"])
	}

	data := tableDataRows(t, s, "analytics", "autodetected_load")
	if data["totalRows"] != "2" {
		t.Fatalf("expected 2 rows, got totalRows=%v", data["totalRows"])
	}
}

func TestLoadJobWithoutSchemaOrAutodetectFailsExplicitly(t *testing.T) {
	s := newTestServer()
	dir := t.TempDir()
	path := filepath.Join(dir, "rows.ndjson")
	_ = os.WriteFile(path, []byte(`{"id": 1}`+"\n"), 0o600)

	body, _ := json.Marshal(map[string]any{
		"configuration": map[string]any{
			"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": "analytics", "tableId": "no_schema_load"},
				"sourceUris":       []any{path},
				"sourceFormat":     "NEWLINE_DELIMITED_JSON",
			},
		},
	})
	jobOut := runJobAndFetch(t, s, string(body))
	status := jobOut["status"].(map[string]any)
	if status["errorResult"] == nil {
		t.Fatalf("expected an explicit error when schema is missing and autodetect is not set")
	}
}
