package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTablesInsertGetDeleteLifecycle(t *testing.T) {
	s := newTestServer()

	createDatasetBody := `{"datasetReference":{"datasetId":"sales"}}`
	createDatasetReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(createDatasetBody))
	createDatasetRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createDatasetRes, createDatasetReq)
	if createDatasetRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on dataset create, got %d", createDatasetRes.Code)
	}

	insertBody := `{"tableReference":{"tableId":"orders"}}`
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/sales/tables", strings.NewReader(insertBody))
	insertReq.Header.Set("Content-Type", "application/json")
	insertRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertRes, insertReq)
	if insertRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", insertRes.Code)
	}

	var inserted map[string]any
	if err := json.NewDecoder(insertRes.Body).Decode(&inserted); err != nil {
		t.Fatalf("decode insert table: %v", err)
	}
	ref := inserted["tableReference"].(map[string]any)
	if ref["tableId"] != "orders" {
		t.Fatalf("expected tableId orders, got %v", ref["tableId"])
	}

	getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/sales/tables/orders", nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on get, got %d", getRes.Code)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/p1/datasets/sales/tables/orders", nil)
	deleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(deleteRes, deleteReq)
	if deleteRes.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", deleteRes.Code)
	}

	missingReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/sales/tables/orders", nil)
	missingRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(missingRes, missingReq)
	if missingRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", missingRes.Code)
	}
}

func TestTablesInsertConflict(t *testing.T) {
	s := newTestServer()

	insertBody := `{"tableReference":{"tableId":"events"}}`
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(insertBody))
	insertReq.Header.Set("Content-Type", "application/json")
	insertRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertRes, insertReq)
	if insertRes.Code != http.StatusConflict {
		t.Fatalf("expected 409 for default table conflict, got %d", insertRes.Code)
	}
}

func TestTablesListPagination(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics/tables?maxResults=2", nil)
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["nextPageToken"] == nil {
		t.Fatalf("expected nextPageToken")
	}
	if body["nextPageToken"] == "2" {
		t.Fatalf("expected opaque nextPageToken, got plain numeric token")
	}
}
