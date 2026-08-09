package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
)

func nullableFields() []tableField {
	return []tableField{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "null_text", Type: "STRING", Mode: "NULLABLE"},
		{Name: "empty_text", Type: "STRING", Mode: "NULLABLE"},
		{Name: "zero_value", Type: "INT64", Mode: "NULLABLE"},
		{Name: "false_value", Type: "BOOL", Mode: "NULLABLE"},
	}
}

func loadNullableNDJSONTable(t *testing.T, s *Server, tableID string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), tableID+".ndjson")
	payload := `{"id":1,"null_text":null,"empty_text":"","zero_value":0,"false_value":false}` + "\n"
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write nullable NDJSON: %v", err)
	}
	fields := make([]any, 0, len(nullableFields()))
	for _, field := range nullableFields() {
		fields = append(fields, map[string]any{"name": field.Name, "type": field.Type, "mode": field.Mode})
	}
	body, err := json.Marshal(map[string]any{
		"configuration": map[string]any{
			"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": "analytics", "tableId": tableID},
				"schema":           map[string]any{"fields": fields},
				"sourceUris":       []any{path},
				"sourceFormat":     "NEWLINE_DELIMITED_JSON",
				"writeDisposition": "WRITE_TRUNCATE",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal nullable load: %v", err)
	}
	job := runJobAndFetch(t, s, string(body))
	status := job["status"].(map[string]any)
	if status["errorResult"] != nil {
		t.Fatalf("nullable load failed: %v", status["errorResult"])
	}
}

func TestNullableValuesRemainDistinctAcrossLoadRESTAndSQL(t *testing.T) {
	s := newTestServer()
	loadNullableNDJSONTable(t, s, "nullable_values")

	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/tabledata/analytics/nullable_values/data", nil)
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("tabledata.list returned %d: %s", res.Code, res.Body.String())
	}
	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode tabledata: %v", err)
	}
	cells := body["rows"].([]any)[0].(map[string]any)["f"].([]any)
	values := make([]any, len(cells))
	for i, cell := range cells {
		values[i] = cell.(map[string]any)["v"]
	}
	if values[0] != "1" || values[1] != nil || values[2] != "" || values[3] != "0" || values[4] != "false" {
		t.Fatalf("expected id/NULL/empty/zero/false to remain distinct, got %#v", values)
	}

	code, query := syncQuery(t, s, "SELECT null_text IS NULL AS is_null, empty_text = '' AS is_empty, zero_value = 0 AS is_zero, false_value = FALSE AS is_false, COALESCE(null_text, 'fallback') AS coalesced FROM analytics.nullable_values")
	if code != http.StatusOK {
		t.Fatalf("nullable query returned %d: %v", code, query)
	}
	queryCells := cellsOf(t, query, 0)
	for i := 0; i < 4; i++ {
		if queryCells[i] != "true" {
			t.Fatalf("expected predicate %d to be true, got %v", i, queryCells)
		}
	}
	if queryCells[4] != "fallback" {
		t.Fatalf("expected COALESCE(NULL) to return fallback, got %v", queryCells[4])
	}

	code, counts := syncQuery(t, s, "SELECT COUNT(null_text) AS null_count, COUNT(empty_text) AS empty_count FROM analytics.nullable_values")
	if code != http.StatusOK {
		t.Fatalf("COUNT query returned %d: %v", code, counts)
	}
	countCells := cellsOf(t, counts, 0)
	if countCells[0] != "0" || countCells[1] != "1" {
		t.Fatalf("expected COUNT(NULL)=0 and COUNT(empty)=1, got %v", countCells)
	}
}

func TestNullableValuesRoundTripNDJSONAvroAndParquet(t *testing.T) {
	schema := nullableFields()
	row, err := parseNDJSONRow(`{"id":1,"null_text":null,"empty_text":"","zero_value":0,"false_value":false}`, schema)
	if err != nil {
		t.Fatalf("parse nullable NDJSON: %v", err)
	}
	rows := [][]string{row}

	ndjson, err := encodeNDJSON(schema, rows)
	if err != nil {
		t.Fatalf("encode nullable NDJSON: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(ndjson))), &decoded); err != nil {
		t.Fatalf("decode extracted NDJSON: %v", err)
	}
	if decoded["null_text"] != nil || decoded["empty_text"] != "" || decoded["zero_value"] != float64(0) || decoded["false_value"] != false {
		t.Fatalf("nullable NDJSON mismatch: %#v", decoded)
	}

	avroPayload, err := encodeAvro(schema, rows, "null")
	if err != nil {
		t.Fatalf("encode nullable Avro: %v", err)
	}
	avroRows, err := parseAvroRows("memory.avro", avroPayload, schema)
	if err != nil {
		t.Fatalf("parse nullable Avro: %v", err)
	}
	assertStoredNullableRow(t, avroRows[0])

	parquetPayload, err := encodeParquet(schema, rows, parquetCodecFor("NONE"))
	if err != nil {
		t.Fatalf("encode nullable Parquet: %v", err)
	}
	parquetRows, err := parseParquetRows("memory.parquet", parquetPayload, schema)
	if err != nil {
		t.Fatalf("parse nullable Parquet: %v", err)
	}
	assertStoredNullableRow(t, parquetRows[0])
}

func assertStoredNullableRow(t *testing.T, row []string) {
	t.Helper()
	if len(row) != 5 || !storedCellIsNull(row[1]) {
		t.Fatalf("expected stored NULL marker in row, got %#v", row)
	}
	empty, emptyNull := loadStoredCell(row[2])
	zero, zeroNull := loadStoredCell(row[3])
	falseValue, falseNull := loadStoredCell(row[4])
	if emptyNull || empty != "" || zeroNull || zero != "0" || falseNull || falseValue != "false" {
		t.Fatalf("expected empty/zero/false stored as values, got %#v", row)
	}
}

func TestStorageReadPreservesNullableValues(t *testing.T) {
	s := newTestServer()
	loadNullableNDJSONTable(t, s, "nullable_storage_read")
	client := newTestStorageReadClient(t, s)
	session, err := client.CreateReadSession(t.Context(), &storagepb.CreateReadSessionRequest{
		Parent:      "projects/p1",
		ReadSession: &storagepb.ReadSession{Table: storageTableName("analytics", "nullable_storage_read")},
	})
	if err != nil {
		t.Fatalf("CreateReadSession: %v", err)
	}
	stream, err := client.ReadRows(t.Context(), &storagepb.ReadRowsRequest{ReadStream: session.GetStreams()[0].GetName()})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	rows := readAllStorageRows(t, response)
	if len(rows) != 1 || rows[0]["null_text"] != nil || rows[0]["empty_text"] != "" || rows[0]["zero_value"] != int64(0) || rows[0]["false_value"] != false {
		t.Fatalf("Storage Read nullable mismatch: %#v", rows)
	}
}

func TestLoadRejectsNullOrMissingRequiredField(t *testing.T) {
	for _, payload := range []string{`{"optional":"x"}` + "\n", `{"required":null,"optional":"x"}` + "\n"} {
		t.Run(payload, func(t *testing.T) {
			s := newTestServer()
			path := filepath.Join(t.TempDir(), "required.ndjson")
			if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(map[string]any{"configuration": map[string]any{"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": "analytics", "tableId": "required_values"},
				"schema": map[string]any{"fields": []any{
					map[string]any{"name": "required", "type": "STRING", "mode": "REQUIRED"},
					map[string]any{"name": "optional", "type": "STRING", "mode": "NULLABLE"},
				}},
				"sourceUris": []any{path}, "sourceFormat": "NEWLINE_DELIMITED_JSON", "writeDisposition": "WRITE_TRUNCATE",
			}}})
			job := runJobAndFetch(t, s, string(body))
			status := job["status"].(map[string]any)
			if status["errorResult"] == nil || !strings.Contains(strings.ToUpper(status["errorResult"].(map[string]any)["message"].(string)), "REQUIRED") {
				t.Fatalf("expected explicit REQUIRED error, got %v", status)
			}
		})
	}
}

func TestStoredNullMarkerCannotCollideWithLiteralString(t *testing.T) {
	for _, literal := range []string{storedNullCell, storedEscapedCellPrefix + "user-data"} {
		stored := scalarValueToString(literal)
		value, isNull := loadStoredCell(stored)
		if isNull || value != literal {
			t.Fatalf("literal %q collided with internal nullable encoding: stored=%q value=%q null=%v", literal, stored, value, isNull)
		}
		if rendered := renderCellForREST(tableField{Type: "STRING"}, stored); rendered != literal {
			t.Fatalf("literal %q rendered as %#v", literal, rendered)
		}
	}
}
