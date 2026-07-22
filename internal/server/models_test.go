package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestModelsInsertGetPatchDeleteLifecycle(t *testing.T) {
	s := newTestServer()

	createDatasetReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"ml_models"}}`))
	createDatasetRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createDatasetRes, createDatasetReq)
	if createDatasetRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on dataset create, got %d", createDatasetRes.Code)
	}

	insertBody := `{"modelReference":{"modelId":"churn"},"modelType":"LOGISTIC_REGRESSION","friendlyName":"Churn model","labels":{"team":"growth"}}`
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/ml_models/models", strings.NewReader(insertBody))
	insertReq.Header.Set("Content-Type", "application/json")
	insertRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertRes, insertReq)
	if insertRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", insertRes.Code, insertRes.Body.String())
	}

	var inserted map[string]any
	if err := json.NewDecoder(insertRes.Body).Decode(&inserted); err != nil {
		t.Fatalf("decode insert: %v", err)
	}
	if inserted["modelType"] != "LOGISTIC_REGRESSION" {
		t.Fatalf("unexpected modelType: %v", inserted["modelType"])
	}
	if _, hasTraining := inserted["trainingRuns"]; hasTraining {
		t.Fatalf("model resource must not fabricate trainingRuns: %v", inserted)
	}

	dupReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/ml_models/models", strings.NewReader(insertBody))
	dupReq.Header.Set("Content-Type", "application/json")
	dupRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(dupRes, dupReq)
	if dupRes.Code != http.StatusConflict {
		t.Fatalf("expected 409 on duplicate insert, got %d", dupRes.Code)
	}

	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/ml_models/models/churn", strings.NewReader(`{"description":"predicts churn risk"}`))
	patchReq.Header.Set("Content-Type", "application/json")
	patchRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchRes, patchReq)
	if patchRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on patch, got %d: %s", patchRes.Code, patchRes.Body.String())
	}
	var patched map[string]any
	if err := json.NewDecoder(patchRes.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	if patched["description"] != "predicts churn risk" {
		t.Fatalf("expected description patched, got %v", patched["description"])
	}
	labels, ok := patched["labels"].(map[string]any)
	if !ok || labels["team"] != "growth" {
		t.Fatalf("expected labels unchanged by partial patch, got %v", patched["labels"])
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/p1/datasets/ml_models/models/churn", nil)
	deleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(deleteRes, deleteReq)
	if deleteRes.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on delete, got %d", deleteRes.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/ml_models/models/churn", nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", getRes.Code)
	}
}

func TestModelsRequireExistingDataset(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/does_not_exist/models", nil)
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for models under missing dataset, got %d", res.Code)
	}
}
