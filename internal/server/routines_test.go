package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRoutinesInsertGetPatchDeleteLifecycle(t *testing.T) {
	s := newTestServer()

	createDatasetReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"analytics_udfs"}}`))
	createDatasetRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createDatasetRes, createDatasetReq)
	if createDatasetRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on dataset create, got %d", createDatasetRes.Code)
	}

	insertBody := `{"routineReference":{"routineId":"add_one"},"routineType":"SCALAR_FUNCTION","language":"SQL","definitionBody":"x + 1"}`
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics_udfs/routines", strings.NewReader(insertBody))
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
	if inserted["routineType"] != "SCALAR_FUNCTION" || inserted["definitionBody"] != "x + 1" {
		t.Fatalf("unexpected inserted routine: %v", inserted)
	}

	dupReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics_udfs/routines", strings.NewReader(insertBody))
	dupReq.Header.Set("Content-Type", "application/json")
	dupRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(dupRes, dupReq)
	if dupRes.Code != http.StatusConflict {
		t.Fatalf("expected 409 on duplicate insert, got %d", dupRes.Code)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics_udfs/routines", nil)
	listRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(listRes, listReq)
	if listRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on list, got %d", listRes.Code)
	}
	var listed map[string]any
	if err := json.NewDecoder(listRes.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if routines, ok := listed["routines"].([]any); !ok || len(routines) != 1 {
		t.Fatalf("expected 1 routine in list, got %v", listed["routines"])
	}

	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/analytics_udfs/routines/add_one", strings.NewReader(`{"description":"adds one to x"}`))
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
	if patched["description"] != "adds one to x" {
		t.Fatalf("expected description patched, got %v", patched["description"])
	}
	if patched["definitionBody"] != "x + 1" {
		t.Fatalf("expected definitionBody unchanged by partial patch, got %v", patched["definitionBody"])
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/p1/datasets/analytics_udfs/routines/add_one", nil)
	deleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(deleteRes, deleteReq)
	if deleteRes.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on delete, got %d", deleteRes.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics_udfs/routines/add_one", nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", getRes.Code)
	}
}

func TestRoutinesInsertRequiresDefinitionBody(t *testing.T) {
	s := newTestServer()
	createDatasetReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"udfs"}}`))
	s.Handler().ServeHTTP(httptest.NewRecorder(), createDatasetReq)

	body := `{"routineReference":{"routineId":"broken"}}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/udfs/routines", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without definitionBody, got %d", res.Code)
	}
}

func TestRoutinesInsertAndPatchRoundTripArguments(t *testing.T) {
	s := newTestServer()
	createDatasetReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"analytics_udfs_args"}}`))
	s.Handler().ServeHTTP(httptest.NewRecorder(), createDatasetReq)

	insertBody := `{"routineReference":{"routineId":"add_two"},"routineType":"SCALAR_FUNCTION","language":"SQL","definitionBody":"x + y","arguments":[{"name":"x","dataType":"INT64"},{"name":"y","dataType":"INT64"}]}`
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics_udfs_args/routines", strings.NewReader(insertBody))
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
	args, ok := inserted["arguments"].([]any)
	if !ok || len(args) != 2 {
		t.Fatalf("expected 2 arguments in insert response, got %v", inserted["arguments"])
	}
	first := args[0].(map[string]any)
	if first["name"] != "x" || first["dataType"] != "INT64" {
		t.Fatalf("unexpected first argument: %v", first)
	}

	// Patching an unrelated field must not clear arguments.
	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/analytics_udfs_args/routines/add_two", strings.NewReader(`{"description":"adds two numbers"}`))
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
	if args, ok := patched["arguments"].([]any); !ok || len(args) != 2 {
		t.Fatalf("expected arguments to survive an unrelated patch, got %v", patched["arguments"])
	}

	// Explicitly patching arguments replaces them.
	patchArgsReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/analytics_udfs_args/routines/add_two", strings.NewReader(`{"arguments":[{"name":"z","dataType":"FLOAT64"}]}`))
	patchArgsReq.Header.Set("Content-Type", "application/json")
	patchArgsRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchArgsRes, patchArgsReq)
	if patchArgsRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on arguments patch, got %d: %s", patchArgsRes.Code, patchArgsRes.Body.String())
	}
	var patchedArgs map[string]any
	if err := json.NewDecoder(patchArgsRes.Body).Decode(&patchedArgs); err != nil {
		t.Fatalf("decode arguments patch: %v", err)
	}
	newArgs, ok := patchedArgs["arguments"].([]any)
	if !ok || len(newArgs) != 1 {
		t.Fatalf("expected arguments replaced with 1 entry, got %v", patchedArgs["arguments"])
	}
	if newArgs[0].(map[string]any)["name"] != "z" {
		t.Fatalf("unexpected replaced argument: %v", newArgs[0])
	}
}

func TestRoutinesRequireExistingDataset(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/does_not_exist/routines", nil)
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for routines under missing dataset, got %d", res.Code)
	}
}
