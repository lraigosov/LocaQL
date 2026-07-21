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

func TestTablesPatchAndUpdateDifferentiated(t *testing.T) {
	s := newTestServer()

	createBody := `{"tableReference":{"tableId":"sessions"},"friendlyName":"Sessions","description":"raw","labels":{"team":"analytics"}}`
	createReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	createRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on create, got %d", createRes.Code)
	}

	patchBody := `{"friendlyName":"Sessions Curated"}`
	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/analytics/tables/sessions", strings.NewReader(patchBody))
	patchReq.Header.Set("Content-Type", "application/json")
	patchRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchRes, patchReq)
	if patchRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on patch, got %d", patchRes.Code)
	}

	var patched map[string]any
	if err := json.NewDecoder(patchRes.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	if patched["friendlyName"] != "Sessions Curated" {
		t.Fatalf("expected friendlyName patched")
	}
	if patched["description"] != "raw" {
		t.Fatalf("expected description preserved after patch")
	}

	updateBody := `{"friendlyName":"Sessions Published","description":"published","labels":{"owner":"data"}}`
	updateReq := httptest.NewRequest(http.MethodPut, "/bigquery/v2/projects/p1/datasets/analytics/tables/sessions", strings.NewReader(updateBody))
	updateReq.Header.Set("Content-Type", "application/json")
	updateRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(updateRes, updateReq)
	if updateRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on update, got %d", updateRes.Code)
	}

	var updated map[string]any
	if err := json.NewDecoder(updateRes.Body).Decode(&updated); err != nil {
		t.Fatalf("decode update: %v", err)
	}
	if updated["friendlyName"] != "Sessions Published" {
		t.Fatalf("expected friendlyName updated")
	}
	if updated["description"] != "published" {
		t.Fatalf("expected description updated")
	}
	labels := updated["labels"].(map[string]any)
	if len(labels) != 1 || labels["owner"] != "data" {
		t.Fatalf("expected labels replaced by update")
	}
}

func TestTablesETagAndTimestamps(t *testing.T) {
	s := newTestServer()

	getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics/tables/events", nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on get, got %d", getRes.Code)
	}

	var got map[string]any
	if err := json.NewDecoder(getRes.Body).Decode(&got); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if got["creationTime"] == nil || got["lastModifiedTime"] == nil {
		t.Fatalf("expected creationTime and lastModifiedTime")
	}
	if got["etag"] == nil {
		t.Fatalf("expected etag in table resource")
	}

	tableETag := getRes.Header().Get("ETag")
	if tableETag == "" {
		t.Fatalf("expected table ETag header")
	}

	getNotModifiedReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics/tables/events", nil)
	getNotModifiedReq.Header.Set("If-None-Match", tableETag)
	getNotModifiedRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getNotModifiedRes, getNotModifiedReq)
	if getNotModifiedRes.Code != http.StatusNotModified {
		t.Fatalf("expected 304 on matching table ETag, got %d", getNotModifiedRes.Code)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics/tables", nil)
	listRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(listRes, listReq)
	if listRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on list, got %d", listRes.Code)
	}

	listETag := listRes.Header().Get("ETag")
	if listETag == "" {
		t.Fatalf("expected list ETag header")
	}

	listNotModifiedReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics/tables", nil)
	listNotModifiedReq.Header.Set("If-None-Match", listETag)
	listNotModifiedRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(listNotModifiedRes, listNotModifiedReq)
	if listNotModifiedRes.Code != http.StatusNotModified {
		t.Fatalf("expected 304 on matching list ETag, got %d", listNotModifiedRes.Code)
	}
}
