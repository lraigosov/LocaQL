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

func TestDatasetsListETag(t *testing.T) {
	s := newTestServer()

	// Initial list to get ETag
	req1 := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets", nil)
	res1 := httptest.NewRecorder()
	s.Handler().ServeHTTP(res1, req1)
	etag := res1.Header().Get("ETag")
	if etag == "" {
		t.Fatalf("expected ETag header")
	}

	// Request with If-None-Match
	req2 := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets", nil)
	req2.Header.Set("If-None-Match", etag)
	res2 := httptest.NewRecorder()
	s.Handler().ServeHTTP(res2, req2)
	if res2.Code != http.StatusNotModified {
		t.Fatalf("expected 304, got %d", res2.Code)
	}

	// Modify datasets
	body := `{"datasetReference":{"datasetId":"new_ds"}}`
	req3 := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(body))
	req3.Header.Set("Content-Type", "application/json")
	s.Handler().ServeHTTP(httptest.NewRecorder(), req3)

	// Request again with old ETag
	req4 := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets", nil)
	req4.Header.Set("If-None-Match", etag)
	res4 := httptest.NewRecorder()
	s.Handler().ServeHTTP(res4, req4)
	if res4.Code != http.StatusOK {
		t.Fatalf("expected 200 after modification, got %d", res4.Code)
	}
}

func TestDatasetsPatchPartialUpdate(t *testing.T) {
	s := newTestServer()

	createBody := `{"datasetReference":{"datasetId":"marketing"},"friendlyName":"Marketing","location":"US","labels":{"team":"growth"}}`
	createReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	createRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on create, got %d", createRes.Code)
	}

	patchBody := `{"friendlyName":"Marketing Analytics","labels":{"team":"analytics","owner":"data"}}`
	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/marketing", strings.NewReader(patchBody))
	patchReq.Header.Set("Content-Type", "application/json")
	patchRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchRes, patchReq)
	if patchRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on patch, got %d", patchRes.Code)
	}

	var patched map[string]any
	if err := json.NewDecoder(patchRes.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patch response: %v", err)
	}
	if patched["friendlyName"] != "Marketing Analytics" {
		t.Fatalf("expected friendlyName to be updated")
	}
	if patched["location"] != "US" {
		t.Fatalf("expected location to remain unchanged")
	}
	labels, ok := patched["labels"].(map[string]any)
	if !ok {
		t.Fatalf("expected labels in response")
	}
	if labels["team"] != "analytics" || labels["owner"] != "data" {
		t.Fatalf("expected labels to be patched, got %#v", labels)
	}
}

func TestDatasetDeleteRequiresDeleteContentsWhenTablesExist(t *testing.T) {
	s := newTestServer()

	createDatasetReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"warehouse"}}`))
	createDatasetRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createDatasetRes, createDatasetReq)
	if createDatasetRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on dataset create, got %d", createDatasetRes.Code)
	}

	insertTableReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/warehouse/tables", strings.NewReader(`{"tableReference":{"tableId":"orders"}}`))
	insertTableReq.Header.Set("Content-Type", "application/json")
	insertTableRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertTableRes, insertTableReq)
	if insertTableRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on table create, got %d", insertTableRes.Code)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/p1/datasets/warehouse", nil)
	deleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(deleteRes, deleteReq)
	if deleteRes.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when dataset still has tables, got %d: %s", deleteRes.Code, deleteRes.Body.String())
	}

	deleteWithContentsReq := httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/p1/datasets/warehouse?deleteContents=true", nil)
	deleteWithContentsRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(deleteWithContentsRes, deleteWithContentsReq)
	if deleteWithContentsRes.Code != http.StatusNoContent {
		t.Fatalf("expected 204 with deleteContents=true, got %d: %s", deleteWithContentsRes.Code, deleteWithContentsRes.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/warehouse", nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after cascade delete, got %d", getRes.Code)
	}
}

func TestDatasetUndeleteRestoresMetadataNotContents(t *testing.T) {
	s := newTestServer()

	createReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"restorable"},"friendlyName":"Restorable","labels":{"team":"data"}}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on create, got %d", createRes.Code)
	}

	insertTableReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/restorable/tables", strings.NewReader(`{"tableReference":{"tableId":"orders"}}`))
	insertTableReq.Header.Set("Content-Type", "application/json")
	insertTableRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertTableRes, insertTableReq)
	if insertTableRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on table create, got %d", insertTableRes.Code)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/p1/datasets/restorable?deleteContents=true", nil)
	deleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(deleteRes, deleteReq)
	if deleteRes.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", deleteRes.Code)
	}

	undeleteReq := httptest.NewRequest(http.MethodPost, "/_emulator/datasets/undelete", strings.NewReader(`{"projectId":"p1","datasetId":"restorable"}`))
	undeleteReq.Header.Set("Content-Type", "application/json")
	undeleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(undeleteRes, undeleteReq)
	if undeleteRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on undelete, got %d: %s", undeleteRes.Code, undeleteRes.Body.String())
	}

	var restored map[string]any
	if err := json.NewDecoder(undeleteRes.Body).Decode(&restored); err != nil {
		t.Fatalf("decode undelete response: %v", err)
	}
	if restored["friendlyName"] != "Restorable" {
		t.Fatalf("expected friendlyName restored, got %v", restored["friendlyName"])
	}
	labels, ok := restored["labels"].(map[string]any)
	if !ok || labels["team"] != "data" {
		t.Fatalf("expected labels restored, got %v", restored["labels"])
	}

	tableGetReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/restorable/tables/orders", nil)
	tableGetRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(tableGetRes, tableGetReq)
	if tableGetRes.Code != http.StatusNotFound {
		t.Fatalf("expected undelete to not restore table contents, got %d for orders table", tableGetRes.Code)
	}
}

func TestDatasetUndeleteFailsWithoutTombstone(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost, "/_emulator/datasets/undelete", strings.NewReader(`{"projectId":"p1","datasetId":"never_deleted"}`))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404 without a tombstone, got %d", res.Code)
	}
}

func TestDatasetUndeleteFailsIfDatasetAlreadyExists(t *testing.T) {
	s := newTestServer()

	createReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"reused"}}`))
	createRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on create, got %d", createRes.Code)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/p1/datasets/reused", nil)
	deleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(deleteRes, deleteReq)
	if deleteRes.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", deleteRes.Code)
	}

	recreateReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"reused"}}`))
	recreateRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(recreateRes, recreateReq)
	if recreateRes.Code != http.StatusOK {
		t.Fatalf("expected 200 recreating dataset, got %d", recreateRes.Code)
	}

	undeleteReq := httptest.NewRequest(http.MethodPost, "/_emulator/datasets/undelete", strings.NewReader(`{"projectId":"p1","datasetId":"reused"}`))
	undeleteReq.Header.Set("Content-Type", "application/json")
	undeleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(undeleteRes, undeleteReq)
	if undeleteRes.Code != http.StatusConflict {
		t.Fatalf("expected 409 when dataset already exists, got %d", undeleteRes.Code)
	}
}

func TestDatasetsDefaultTableExpirationMsRoundTrip(t *testing.T) {
	s := newTestServer()

	createReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"expiring"},"defaultTableExpirationMs":"3600000"}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", createRes.Code, createRes.Body.String())
	}

	var created map[string]any
	if err := json.NewDecoder(createRes.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created["defaultTableExpirationMs"] != "3600000" {
		t.Fatalf("expected defaultTableExpirationMs '3600000' from string input, got %v", created["defaultTableExpirationMs"])
	}

	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/expiring", strings.NewReader(`{"defaultTableExpirationMs":7200000}`))
	patchReq.Header.Set("Content-Type", "application/json")
	patchRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchRes, patchReq)
	if patchRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", patchRes.Code, patchRes.Body.String())
	}

	var patched map[string]any
	if err := json.NewDecoder(patchRes.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patch response: %v", err)
	}
	if patched["defaultTableExpirationMs"] != "7200000" {
		t.Fatalf("expected defaultTableExpirationMs '7200000' from numeric patch input, got %v", patched["defaultTableExpirationMs"])
	}
}

func TestDatasetsInsertRejectsInvalidDefaultTableExpirationMs(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"bad_expiry"},"defaultTableExpirationMs":"not-a-number"}`))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-numeric defaultTableExpirationMs, got %d", res.Code)
	}
}

func TestDatasetsPatchNotFound(t *testing.T) {
	s := newTestServer()

	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/missing", strings.NewReader(`{"friendlyName":"x"}`))
	patchReq.Header.Set("Content-Type", "application/json")
	patchRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchRes, patchReq)

	if patchRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", patchRes.Code)
	}
}
