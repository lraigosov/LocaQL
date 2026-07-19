package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestJobsInsertAndGetLifecycle(t *testing.T) {
	s := newTestServer()

	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", nil)
	insertRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertRes, insertReq)
	if insertRes.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", insertRes.Code)
	}

	var created map[string]any
	if err := json.NewDecoder(insertRes.Body).Decode(&created); err != nil {
		t.Fatalf("decode created job: %v", err)
	}
	jobRef := created["jobReference"].(map[string]any)
	jobID := jobRef["jobId"].(string)

	time.Sleep(180 * time.Millisecond)

	getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs/"+jobID, nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRes.Code)
	}

	var got map[string]any
	if err := json.NewDecoder(getRes.Body).Decode(&got); err != nil {
		t.Fatalf("decode get job: %v", err)
	}
	status := got["status"].(map[string]any)
	if status["state"] != "DONE" {
		t.Fatalf("expected state DONE, got %v", status["state"])
	}
}

func TestJobsCancel(t *testing.T) {
	s := newTestServer()

	insertReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", nil)
	insertRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertRes, insertReq)
	if insertRes.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", insertRes.Code)
	}

	var created map[string]any
	if err := json.NewDecoder(insertRes.Body).Decode(&created); err != nil {
		t.Fatalf("decode created job: %v", err)
	}
	jobRef := created["jobReference"].(map[string]any)
	jobID := jobRef["jobId"].(string)

	cancelReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs/"+jobID+"/cancel", nil)
	cancelRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(cancelRes, cancelReq)
	if cancelRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", cancelRes.Code)
	}

	time.Sleep(50 * time.Millisecond)

	getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs/"+jobID, nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRes.Code)
	}

	var got map[string]any
	if err := json.NewDecoder(getRes.Body).Decode(&got); err != nil {
		t.Fatalf("decode get job: %v", err)
	}
	status := got["status"].(map[string]any)
	if status["state"] != "DONE" {
		t.Fatalf("expected state DONE after cancel, got %v", status["state"])
	}
	if status["errorResult"] == nil {
		t.Fatalf("expected errorResult on cancelled job")
	}
}

func TestJobsIdempotencyByRequestID(t *testing.T) {
	s := newTestServer()

	firstReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs?requestId=req_a", nil)
	firstRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(firstRes, firstReq)
	if firstRes.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", firstRes.Code)
	}

	secondReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs?requestId=req_a", nil)
	secondRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(secondRes, secondReq)
	if secondRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", secondRes.Code)
	}

	var first map[string]any
	if err := json.NewDecoder(firstRes.Body).Decode(&first); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	var second map[string]any
	if err := json.NewDecoder(secondRes.Body).Decode(&second); err != nil {
		t.Fatalf("decode second: %v", err)
	}

	fRef := first["jobReference"].(map[string]any)
	sRef := second["jobReference"].(map[string]any)
	if fRef["jobId"] != sRef["jobId"] {
		t.Fatalf("expected same jobId for idempotent request")
	}
}

func TestJobsListByStateFilter(t *testing.T) {
	s := newTestServer()

	req1 := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", nil)
	res1 := httptest.NewRecorder()
	s.Handler().ServeHTTP(res1, req1)
	if res1.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res1.Code)
	}

	time.Sleep(200 * time.Millisecond)

	listReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs?stateFilter=DONE&maxResults=10", nil)
	listRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(listRes, listReq)
	if listRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", listRes.Code)
	}

	var body struct {
		Jobs []map[string]any `json:"jobs"`
	}
	if err := json.NewDecoder(listRes.Body).Decode(&body); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(body.Jobs) == 0 {
		t.Fatalf("expected at least one DONE job")
	}
}

func TestJobsScriptCreatesChildJobs(t *testing.T) {
	s := newTestServer()
	body := `{"configuration":{"query":{"query":"SELECT 1; SELECT 2;"}}}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs?userEmail=user@example.com", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.Code)
	}

	var out struct {
		Job      map[string]any   `json:"job"`
		Children []map[string]any `json:"children"`
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode script response: %v", err)
	}
	if len(out.Children) != 2 {
		t.Fatalf("expected 2 child jobs, got %d", len(out.Children))
	}

	parentRef := out.Job["jobReference"].(map[string]any)
	parentID := parentRef["jobId"].(string)
	for _, child := range out.Children {
		if child["parentJobId"] != parentID {
			t.Fatalf("expected child parentJobId %s, got %v", parentID, child["parentJobId"])
		}
	}
}

func TestJobsListByUserRangeAndParent(t *testing.T) {
	s := newTestServer()

	body := `{"configuration":{"query":{"query":"SELECT 1; SELECT 2;"}}}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs?userEmail=owner@example.com", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.Code)
	}

	var out struct {
		Job      map[string]any   `json:"job"`
		Children []map[string]any `json:"children"`
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode script response: %v", err)
	}
	parentRef := out.Job["jobReference"].(map[string]any)
	parentID := parentRef["jobId"].(string)

	now := time.Now().UTC().UnixMilli()
	listReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs?userEmail=owner@example.com&parentJobId="+parentID+"&minCreationTime="+strconv.FormatInt(now-10000, 10)+"&maxCreationTime="+strconv.FormatInt(now+10000, 10), nil)
	listRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(listRes, listReq)
	if listRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", listRes.Code)
	}

	var listOut struct {
		Jobs []map[string]any `json:"jobs"`
	}
	if err := json.NewDecoder(listRes.Body).Decode(&listOut); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listOut.Jobs) != 2 {
		t.Fatalf("expected 2 filtered child jobs, got %d", len(listOut.Jobs))
	}
}

func TestRequestIDTTLAllowsNewJobAfterExpiration(t *testing.T) {
	js := newJobServiceWithTTL(1 * time.Millisecond)
	first, created := js.insert(jobInsertOptions{ProjectID: "p1", RequestID: "rq1"})
	if !created {
		t.Fatalf("expected first insert to create job")
	}

	time.Sleep(3 * time.Millisecond)
	second, createdAgain := js.insert(jobInsertOptions{ProjectID: "p1", RequestID: "rq1"})
	if !createdAgain {
		t.Fatalf("expected second insert to create a new job after TTL expiration")
	}
	if first.JobID == second.JobID {
		t.Fatalf("expected different job IDs after TTL expiration")
	}
}

func TestJobsExecutorTypeAndStatistics(t *testing.T) {
	s := newTestServer()
	body := `{"configuration":{"copy":{"sourceTables":[],"destinationTable":{}}}}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.Code)
	}

	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode create copy job: %v", err)
	}
	jobRef := out["jobReference"].(map[string]any)
	jobID := jobRef["jobId"].(string)

	time.Sleep(160 * time.Millisecond)

	getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs/"+jobID, nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRes.Code)
	}

	var got map[string]any
	if err := json.NewDecoder(getRes.Body).Decode(&got); err != nil {
		t.Fatalf("decode get copy job: %v", err)
	}
	if got["jobType"] != "copy" {
		t.Fatalf("expected jobType copy, got %v", got["jobType"])
	}
	stats := got["statistics"].(map[string]any)
	sim := stats["simulation"].(map[string]any)
	if sim["enabled"] != true {
		t.Fatalf("expected simulation enabled")
	}
	if sim["executor"] != "copy" {
		t.Fatalf("expected copy executor, got %v", sim["executor"])
	}
}

func TestJobsPersistenceAcrossRestart(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "jobs", "state.json")
	firstService := newJobServiceWithPersistence(storePath)
	job, created := firstService.insert(jobInsertOptions{ProjectID: "p1", RequestID: "persist-req", JobType: "query"})
	if !created {
		t.Fatalf("expected new job creation")
	}

	time.Sleep(20 * time.Millisecond)

	secondService := newJobServiceWithPersistence(storePath)
	loaded, ok := secondService.get("p1", job.JobID)
	if !ok {
		t.Fatalf("expected persisted job to be loaded after restart")
	}
	if loaded.JobID != job.JobID {
		t.Fatalf("expected same job ID after restart")
	}
}
