package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lraigosov/LocaQL/internal/capabilities"
)

func newTestServer() *Server {
	return New(capabilities.Registry{Capabilities: map[string]capabilities.Entry{"emulator.health": {Status: "supported", Fidelity: "high"}}})
}

func TestDatasetsListPagination(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets?maxResults=2", nil)
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

func TestJobsListPagination(t *testing.T) {
	s := newTestServer()

	for i := 0; i < 5; i++ {
		createReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", nil)
		createRes := httptest.NewRecorder()
		s.Handler().ServeHTTP(createRes, createReq)
		if createRes.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d", createRes.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs?maxResults=2&pageToken=2", nil)
	res := httptest.NewRecorder()

	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var body struct {
		Jobs []any `json:"jobs"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(body.Jobs))
	}
}

func TestJobsListPaginationWithOpaqueToken(t *testing.T) {
	s := newTestServer()

	for i := 0; i < 5; i++ {
		createReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", nil)
		createRes := httptest.NewRecorder()
		s.Handler().ServeHTTP(createRes, createReq)
		if createRes.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d", createRes.Code)
		}
	}

	firstReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs?maxResults=2", nil)
	firstRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(firstRes, firstReq)
	if firstRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", firstRes.Code)
	}

	var firstBody struct {
		Jobs          []any  `json:"jobs"`
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.NewDecoder(firstRes.Body).Decode(&firstBody); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if firstBody.NextPageToken == "" {
		t.Fatalf("expected nextPageToken on first page")
	}
	if firstBody.NextPageToken == "2" {
		t.Fatalf("expected opaque nextPageToken, got plain numeric token")
	}

	secondReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs?maxResults=2&pageToken="+firstBody.NextPageToken, nil)
	secondRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(secondRes, secondReq)
	if secondRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", secondRes.Code)
	}

	var secondBody struct {
		Jobs []any `json:"jobs"`
	}
	if err := json.NewDecoder(secondRes.Body).Decode(&secondBody); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(secondBody.Jobs) != 2 {
		t.Fatalf("expected 2 jobs on second page, got %d", len(secondBody.Jobs))
	}
}

func TestTableDataListPagination(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/tabledata/analytics/users/data?startIndex=1&maxResults=2", nil)
	res := httptest.NewRecorder()

	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var body struct {
		Rows          []any  `json:"rows"`
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(body.Rows))
	}
	if body.NextPageToken == "" {
		t.Fatalf("expected nextPageToken")
	}
}

// TestTableDataListFinalPageOmitsPageToken guards against a real infinite-loop
// bug: the response used to always include a "pageToken" key (echoing the
// request's own start index) even on the last page. google-cloud-bigquery's
// list_rows() constructs its RowIterator with next_token="pageToken" for
// this endpoint specifically (not "nextPageToken", unlike most other list
// APIs) — it checks for that key's mere presence to decide whether to fetch
// another page. An unconditional "pageToken" therefore made the official
// client re-request the same final page forever. Both "pageToken" and
// "nextPageToken" must be entirely absent once there's nothing left to page
// through.
func TestTableDataListFinalPageOmitsPageToken(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics/tables/users/data?maxResults=100", nil)
	res := httptest.NewRecorder()

	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", res.Code, res.Body.String())
	}

	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, present := body["pageToken"]; present {
		t.Fatalf("expected no pageToken on the final page, got %v", body["pageToken"])
	}
	if _, present := body["nextPageToken"]; present {
		t.Fatalf("expected no nextPageToken on the final page, got %v", body["nextPageToken"])
	}
}

// TestTableDataListRealBigQueryPath guards against a routing regression: real
// BigQuery's tabledata.list REST endpoint is GET
// .../datasets/{datasetId}/tables/{tableId}/data (6 path segments under
// .../projects/{p}/), which is what google-cloud-bigquery's list_rows()
// actually requests. The made-up .../tabledata/{datasetId}/{tableId}/data
// shape covered by TestTableDataListPagination above is an internal alias
// only the bundled UI and its own tests ever call — official clients 404'd
// against this server before dispatchDatasetSubResource grew a "tables" case
// for the trailing "/data" segment, even though the table/rows existed and
// getTable succeeded for that same table.
func TestTableDataListRealBigQueryPath(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics/tables/users/data?startIndex=1&maxResults=2", nil)
	res := httptest.NewRecorder()

	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", res.Code, res.Body.String())
	}

	var body struct {
		Rows          []any  `json:"rows"`
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(body.Rows))
	}
}

func TestTableDataListRealBigQueryPathRejectsNonGET(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables/users/data", nil)
	res := httptest.NewRecorder()

	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d: %s", res.Code, res.Body.String())
	}
}

func TestTableDataListRealBigQueryPathUnknownTable404s(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics/tables/does-not-exist/data", nil)
	res := httptest.NewRecorder()

	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", res.Code, res.Body.String())
	}
}
