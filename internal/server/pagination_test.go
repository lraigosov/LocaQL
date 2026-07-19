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

func TestTableDataListPagination(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/tabledata/ds1/t1/data?startIndex=1&maxResults=2", nil)
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
