package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDatasetsInsertGetDeleteLifecycle(t *testing.T) {
	s := newTestServer()

	insertBody := `{"datasetReference":{"datasetId":"sales"},"friendlyName":"Sales","location":"US","labels":{"team":"analytics"}}`
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(insertBody))
	insertReq.Header.Set("Content-Type", "application/json")
	insertRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertRes, insertReq)
	if insertRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", insertRes.Code)
	}

	var inserted map[string]any
	if err := json.NewDecoder(insertRes.Body).Decode(&inserted); err != nil {
		t.Fatalf("decode insert: %v", err)
	}
	ref := inserted["datasetReference"].(map[string]any)
	if ref["datasetId"] != "sales" {
		t.Fatalf("expected datasetId sales, got %v", ref["datasetId"])
	}

	getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/sales", nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRes.Code)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/p1/datasets/sales", nil)
	deleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(deleteRes, deleteReq)
	if deleteRes.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", deleteRes.Code)
	}

	missingReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/sales", nil)
	missingRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(missingRes, missingReq)
	if missingRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", missingRes.Code)
	}
}

func TestDatasetsInsertConflict(t *testing.T) {
	s := newTestServer()
	body := `{"datasetReference":{"datasetId":"finance"}}`

	firstReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(body))
	firstRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(firstRes, firstReq)
	if firstRes.Code != http.StatusConflict {
		t.Fatalf("expected 409 for default dataset conflict, got %d", firstRes.Code)
	}
}

func TestTablesListReturnsNotFoundForMissingDataset(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/not_exists/tables", nil)
	res := httptest.NewRecorder()

	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", res.Code)
	}
}
