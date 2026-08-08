package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func createStreamingInsertTestTable(t *testing.T, s *Server, datasetID, tableID string, fields []map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"tableReference": map[string]any{"tableId": tableID},
		"schema":         map[string]any{"fields": fields},
	})
	if err != nil {
		t.Fatalf("marshal table body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/"+datasetID+"/tables", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("create table %s: expected 200, got %d: %s", tableID, res.Code, res.Body.String())
	}
}

func insertAllTestRequest(t *testing.T, s *Server, datasetID, tableID string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal insertAll body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/"+datasetID+"/tables/"+tableID+"/insertAll", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	var out map[string]any
	if res.Body.Len() > 0 {
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			t.Fatalf("decode insertAll response: %v (body: %s)", err, res.Body.String())
		}
	}
	return res.Code, out
}

func TestStreamingInsertAppendsRowsRealTime(t *testing.T) {
	s := newTestServer()
	createStreamingInsertTestTable(t, s, "analytics", "streaming_events", []map[string]any{
		{"name": "id", "type": "INT64", "mode": "REQUIRED"},
		{"name": "label", "type": "STRING"},
	})

	code, out := insertAllTestRequest(t, s, "analytics", "streaming_events", map[string]any{
		"rows": []any{
			map[string]any{"json": map[string]any{"id": 1, "label": "a"}},
			map[string]any{"json": map[string]any{"id": 2, "label": "b"}},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if _, hasErrors := out["insertErrors"]; hasErrors {
		t.Fatalf("unexpected insertErrors: %v", out["insertErrors"])
	}

	data := tableDataRows(t, s, "analytics", "streaming_events")
	if data["totalRows"] != "2" {
		t.Fatalf("expected totalRows=2, got %v", data["totalRows"])
	}
}

func TestStreamingInsertNestedRecordMatchesLoadEncoding(t *testing.T) {
	s := newTestServer()
	createStreamingInsertTestTable(t, s, "analytics", "orders", []map[string]any{
		{"name": "id", "type": "INT64", "mode": "REQUIRED"},
		{"name": "address", "type": "RECORD", "fields": []any{
			map[string]any{"name": "city", "type": "STRING"},
			map[string]any{"name": "zip", "type": "STRING"},
		}},
	})

	code, out := insertAllTestRequest(t, s, "analytics", "orders", map[string]any{
		"rows": []any{
			map[string]any{"json": map[string]any{"id": 1, "address": map[string]any{"city": "Bogota", "zip": "110111"}}},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if _, hasErrors := out["insertErrors"]; hasErrors {
		t.Fatalf("unexpected insertErrors: %v", out["insertErrors"])
	}

	data := tableDataRows(t, s, "analytics", "orders")
	cells := recordCellsAt(t, data, 0, 1)
	if got := recordFieldValue(t, cells, 0); got != "Bogota" {
		t.Fatalf("expected nested city=Bogota, got %v", got)
	}
}

func TestStreamingInsertRequiredFieldMissingFailsWholeRequestByDefault(t *testing.T) {
	s := newTestServer()
	createStreamingInsertTestTable(t, s, "analytics", "required_case", []map[string]any{
		{"name": "id", "type": "INT64", "mode": "REQUIRED"},
	})

	code, out := insertAllTestRequest(t, s, "analytics", "required_case", map[string]any{
		"rows": []any{
			map[string]any{"json": map[string]any{}},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200 (insertAll reports errors in-band), got %d", code)
	}
	if out["insertErrors"] == nil {
		t.Fatalf("expected insertErrors for missing REQUIRED field")
	}

	data := tableDataRows(t, s, "analytics", "required_case")
	if data["totalRows"] != "0" {
		t.Fatalf("expected no rows inserted when skipInvalidRows is false, got totalRows=%v", data["totalRows"])
	}
}

func TestStreamingInsertSkipInvalidRowsKeepsValidOnes(t *testing.T) {
	s := newTestServer()
	createStreamingInsertTestTable(t, s, "analytics", "skip_case", []map[string]any{
		{"name": "id", "type": "INT64", "mode": "REQUIRED"},
	})

	code, out := insertAllTestRequest(t, s, "analytics", "skip_case", map[string]any{
		"skipInvalidRows": true,
		"rows": []any{
			map[string]any{"json": map[string]any{}},
			map[string]any{"json": map[string]any{"id": 7}},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if out["insertErrors"] == nil {
		t.Fatalf("expected insertErrors describing the invalid row")
	}

	data := tableDataRows(t, s, "analytics", "skip_case")
	if data["totalRows"] != "1" {
		t.Fatalf("expected 1 row inserted (the valid one), got totalRows=%v", data["totalRows"])
	}
}

func TestStreamingInsertUnknownFieldRejectedByDefault(t *testing.T) {
	s := newTestServer()
	createStreamingInsertTestTable(t, s, "analytics", "unknown_case", []map[string]any{
		{"name": "id", "type": "INT64"},
	})

	code, out := insertAllTestRequest(t, s, "analytics", "unknown_case", map[string]any{
		"rows": []any{
			map[string]any{"json": map[string]any{"id": 1, "extra": "nope"}},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if out["insertErrors"] == nil {
		t.Fatalf("expected insertErrors for unknown field 'extra'")
	}
}

func TestStreamingInsertIgnoreUnknownValuesAllowsExtraFields(t *testing.T) {
	s := newTestServer()
	createStreamingInsertTestTable(t, s, "analytics", "ignore_unknown_case", []map[string]any{
		{"name": "id", "type": "INT64"},
	})

	code, out := insertAllTestRequest(t, s, "analytics", "ignore_unknown_case", map[string]any{
		"ignoreUnknownValues": true,
		"rows": []any{
			map[string]any{"json": map[string]any{"id": 1, "extra": "fine"}},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if _, hasErrors := out["insertErrors"]; hasErrors {
		t.Fatalf("unexpected insertErrors: %v", out["insertErrors"])
	}
}

func TestStreamingInsertDedupesByInsertID(t *testing.T) {
	s := newTestServer()
	createStreamingInsertTestTable(t, s, "analytics", "dedup_case", []map[string]any{
		{"name": "id", "type": "INT64"},
	})

	row := map[string]any{"insertId": "row-1", "json": map[string]any{"id": 42}}

	if code, _ := insertAllTestRequest(t, s, "analytics", "dedup_case", map[string]any{"rows": []any{row}}); code != http.StatusOK {
		t.Fatalf("first insert: expected 200, got %d", code)
	}
	// Retry with the exact same insertId, simulating an at-least-once client
	// retry: it must not double-insert the row.
	if code, _ := insertAllTestRequest(t, s, "analytics", "dedup_case", map[string]any{"rows": []any{row}}); code != http.StatusOK {
		t.Fatalf("retry insert: expected 200, got %d", code)
	}

	data := tableDataRows(t, s, "analytics", "dedup_case")
	if data["totalRows"] != "1" {
		t.Fatalf("expected exactly 1 row after a deduped retry, got totalRows=%v", data["totalRows"])
	}
}

func TestStreamingInsertRejectsTemplateSuffix(t *testing.T) {
	s := newTestServer()
	createStreamingInsertTestTable(t, s, "analytics", "template_case", []map[string]any{
		{"name": "id", "type": "INT64"},
	})

	code, _ := insertAllTestRequest(t, s, "analytics", "template_case", map[string]any{
		"templateSuffix": "_20260101",
		"rows":           []any{map[string]any{"json": map[string]any{"id": 1}}},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unsupported templateSuffix, got %d", code)
	}
}

func TestStreamingInsertMissingTableReturns404(t *testing.T) {
	s := newTestServer()
	code, _ := insertAllTestRequest(t, s, "analytics", "does_not_exist", map[string]any{
		"rows": []any{map[string]any{"json": map[string]any{"id": 1}}},
	})
	if code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", code)
	}
}
