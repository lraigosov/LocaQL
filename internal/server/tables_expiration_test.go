package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTablesInheritsDatasetDefaultTableExpirationMs(t *testing.T) {
	s := newTestServer()
	fixedNow := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	s.tables.now = func() time.Time { return fixedNow }

	createDatasetReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"expiring_inherit"},"defaultTableExpirationMs":"3600000"}`))
	createDatasetRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createDatasetRes, createDatasetReq)
	if createDatasetRes.Code != http.StatusOK {
		t.Fatalf("expected 200 on dataset create, got %d", createDatasetRes.Code)
	}

	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/expiring_inherit/tables", strings.NewReader(`{"tableReference":{"tableId":"orders"}}`))
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
	expected := strconv.FormatInt(fixedNow.Add(1*time.Hour).UnixMilli(), 10)
	if inserted["expirationTime"] != expected {
		t.Fatalf("expected expirationTime %s inherited from dataset default, got %v", expected, inserted["expirationTime"])
	}
}

func TestTablesExplicitExpirationTimeOverridesDatasetDefault(t *testing.T) {
	s := newTestServer()
	createDatasetReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"expiring_override"},"defaultTableExpirationMs":"3600000"}`))
	s.Handler().ServeHTTP(httptest.NewRecorder(), createDatasetReq)

	explicitMs := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	insertBody := fmt.Sprintf(`{"tableReference":{"tableId":"orders"},"expirationTime":"%d"}`, explicitMs)
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/expiring_override/tables", strings.NewReader(insertBody))
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
	if inserted["expirationTime"] != strconv.FormatInt(explicitMs, 10) {
		t.Fatalf("expected explicit expirationTime to override dataset default, got %v", inserted["expirationTime"])
	}
}

// TestTablesExpireAndArePurgedAfterExpirationTime uses a swappable clock
// (s.tables.now) instead of real sleeps, so expiration is deterministic and
// instant rather than a flaky wall-clock wait.
func TestTablesExpireAndArePurgedAfterExpirationTime(t *testing.T) {
	s := newTestServer()
	current := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	s.tables.now = func() time.Time { return current }

	createDatasetReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"expiring_gone"}}`))
	s.Handler().ServeHTTP(httptest.NewRecorder(), createDatasetReq)

	expiresAtMs := current.Add(10 * time.Minute).UnixMilli()
	insertBody := fmt.Sprintf(`{"tableReference":{"tableId":"orders"},"expirationTime":"%d"}`, expiresAtMs)
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/expiring_gone/tables", strings.NewReader(insertBody))
	insertReq.Header.Set("Content-Type", "application/json")
	insertRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertRes, insertReq)
	if insertRes.Code != http.StatusOK {
		t.Fatalf("expected 200 creating table, got %d: %s", insertRes.Code, insertRes.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/expiring_gone/tables/orders", nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusOK {
		t.Fatalf("expected 200 before expiration, got %d", getRes.Code)
	}

	current = current.Add(11 * time.Minute)

	getAfterReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/expiring_gone/tables/orders", nil)
	getAfterRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getAfterRes, getAfterReq)
	if getAfterRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after expiration, got %d: %s", getAfterRes.Code, getAfterRes.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/expiring_gone/tables", nil)
	listRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(listRes, listReq)
	// The dataset also carries the tableService's own demo fixtures (seeded
	// on first touch, unrelated to this feature), so assert the expired
	// table's absence specifically rather than expecting an empty list.
	if strings.Contains(listRes.Body.String(), `"tableId":"orders"`) {
		t.Fatalf("expected expired table excluded from list, got %s", listRes.Body.String())
	}

	// The expired table was purged (not merely hidden): recreating with the
	// same ID must succeed rather than conflict with a lingering record.
	recreateReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/expiring_gone/tables", strings.NewReader(`{"tableReference":{"tableId":"orders"}}`))
	recreateReq.Header.Set("Content-Type", "application/json")
	recreateRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(recreateRes, recreateReq)
	if recreateRes.Code != http.StatusOK {
		t.Fatalf("expected 200 recreating an expired table id, got %d: %s", recreateRes.Code, recreateRes.Body.String())
	}
}

func TestTablesPatchExpirationTimeRoundTrip(t *testing.T) {
	s := newTestServer()
	createDatasetReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"expiring_patch"}}`))
	s.Handler().ServeHTTP(httptest.NewRecorder(), createDatasetReq)
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/expiring_patch/tables", strings.NewReader(`{"tableReference":{"tableId":"orders"}}`))
	insertReq.Header.Set("Content-Type", "application/json")
	s.Handler().ServeHTTP(httptest.NewRecorder(), insertReq)

	newExpirationMs := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/expiring_patch/tables/orders", strings.NewReader(fmt.Sprintf(`{"expirationTime":"%d"}`, newExpirationMs)))
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
	if patched["expirationTime"] != strconv.FormatInt(newExpirationMs, 10) {
		t.Fatalf("expected patched expirationTime, got %v", patched["expirationTime"])
	}
}

func TestDatasetDeleteTableCountExcludesExpiredTables(t *testing.T) {
	s := newTestServer()
	current := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	s.tables.now = func() time.Time { return current }

	createDatasetReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"expiring_delete"}}`))
	s.Handler().ServeHTTP(httptest.NewRecorder(), createDatasetReq)

	expiresAtMs := current.Add(1 * time.Minute).UnixMilli()
	insertBody := fmt.Sprintf(`{"tableReference":{"tableId":"orders"},"expirationTime":"%d"}`, expiresAtMs)
	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/expiring_delete/tables", strings.NewReader(insertBody))
	insertReq.Header.Set("Content-Type", "application/json")
	s.Handler().ServeHTTP(httptest.NewRecorder(), insertReq)

	current = current.Add(2 * time.Minute)

	// Inserting "orders" also seeded the tableService's 4 permanent demo
	// fixtures as a side effect of first touching this dataset (unrelated to
	// this feature), so the dataset is not actually empty — but the expired
	// "orders" table must be excluded from the count that blocks a
	// deleteContents-less delete. If purging were broken, this would report
	// "still contains 5 table(s)" instead of 4.
	deleteReq := httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/p1/datasets/expiring_delete", nil)
	deleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(deleteRes, deleteReq)
	if deleteRes.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 (still contains the 4 non-expiring demo tables), got %d: %s", deleteRes.Code, deleteRes.Body.String())
	}
	if !strings.Contains(deleteRes.Body.String(), "contains 4 table") {
		t.Fatalf("expected the expired table excluded from the count (4, not 5), got %s", deleteRes.Body.String())
	}
}
