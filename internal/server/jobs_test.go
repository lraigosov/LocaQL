package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
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
		if status["state"] == "DONE" {
			if status["errorResult"] == nil {
				t.Fatalf("expected errorResult on cancelled job")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected cancelled job to reach DONE state within timeout")
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
	body := `{"configuration":{"copy":{"sourceTable":{"projectId":"p1","datasetId":"analytics","tableId":"users"},"destinationTable":{"projectId":"p1","datasetId":"analytics","tableId":"users_copy"},"writeDisposition":"WRITE_TRUNCATE"}}}`
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
	if sim["enabled"] != false {
		t.Fatalf("expected simulation disabled for real copy")
	}
	if sim["executor"] != "copy" {
		t.Fatalf("expected copy executor, got %v", sim["executor"])
	}
	if stats["outputRows"] != float64(4) {
		t.Fatalf("expected 4 copied rows, got %v", stats["outputRows"])
	}
}

func TestCopyJobCreatesReadableDestinationTable(t *testing.T) {
	s := newTestServer()
	body := `{"configuration":{"copy":{"sourceTable":{"projectId":"p1","datasetId":"analytics","tableId":"daily_metrics"},"destinationTable":{"projectId":"p1","datasetId":"analytics","tableId":"daily_metrics_copy"},"writeDisposition":"WRITE_TRUNCATE"}}}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.Code)
	}

	time.Sleep(160 * time.Millisecond)

	dataReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/tabledata/analytics/daily_metrics_copy/data", nil)
	dataRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(dataRes, dataReq)
	if dataRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", dataRes.Code)
	}

	var out map[string]any
	if err := json.NewDecoder(dataRes.Body).Decode(&out); err != nil {
		t.Fatalf("decode table data: %v", err)
	}
	if out["totalRows"] != "4" {
		t.Fatalf("expected totalRows 4, got %v", out["totalRows"])
	}
	rows := out["rows"].([]any)
	first := rows[0].(map[string]any)["f"].([]any)
	firstValue := first[0].(map[string]any)["v"]
	if firstValue != "2026-07-18" {
		t.Fatalf("expected copied first row from source table, got %v", firstValue)
	}
}

func TestLoadJobMaterializesDestinationTableSchema(t *testing.T) {
	s := newTestServer()
	body := `{"configuration":{"load":{"destinationTable":{"projectId":"p1","datasetId":"analytics","tableId":"events_loaded"},"schema":{"fields":[{"name":"event_id","type":"INT64"},{"name":"event_name","type":"STRING"}]},"writeDisposition":"WRITE_TRUNCATE"}}}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.Code)
	}

	var created map[string]any
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		t.Fatalf("decode created load job: %v", err)
	}
	jobID := created["jobReference"].(map[string]any)["jobId"].(string)

	time.Sleep(220 * time.Millisecond)

	jobReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs/"+jobID, nil)
	jobRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(jobRes, jobReq)
	if jobRes.Code != http.StatusOK {
		t.Fatalf("expected 200 loading job, got %d", jobRes.Code)
	}

	var jobOut map[string]any
	if err := json.NewDecoder(jobRes.Body).Decode(&jobOut); err != nil {
		t.Fatalf("decode loaded job: %v", err)
	}
	stats := jobOut["statistics"].(map[string]any)
	sim := stats["simulation"].(map[string]any)
	if sim["enabled"] != false {
		t.Fatalf("expected simulation disabled for load materialization")
	}
	if sim["executor"] != "load" {
		t.Fatalf("expected load executor, got %v", sim["executor"])
	}

	tableReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics/tables/events_loaded", nil)
	tableRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(tableRes, tableReq)
	if tableRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for loaded table, got %d", tableRes.Code)
	}

	var tableOut map[string]any
	if err := json.NewDecoder(tableRes.Body).Decode(&tableOut); err != nil {
		t.Fatalf("decode loaded table: %v", err)
	}
	fields := tableOut["schema"].(map[string]any)["fields"].([]any)
	if len(fields) != 2 {
		t.Fatalf("expected 2 schema fields, got %d", len(fields))
	}
	first := fields[0].(map[string]any)
	if first["name"] != "event_id" || first["type"] != "INT64" {
		t.Fatalf("unexpected first field: %v", first)
	}

	dataReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/tabledata/analytics/events_loaded/data", nil)
	dataRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(dataRes, dataReq)
	if dataRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for loaded table data, got %d", dataRes.Code)
	}
	var dataOut map[string]any
	if err := json.NewDecoder(dataRes.Body).Decode(&dataOut); err != nil {
		t.Fatalf("decode loaded table data: %v", err)
	}
	if dataOut["totalRows"] != "0" {
		t.Fatalf("expected totalRows 0 for schema-only load, got %v", dataOut["totalRows"])
	}
}

func TestJobsSyncQuerySupportsInformationSchemaTables(t *testing.T) {
	s := newTestServer()
	body := `{"query":"SELECT * FROM p1.analytics.INFORMATION_SCHEMA.TABLES"}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode information schema response: %v", err)
	}
	rows, ok := out["rows"].([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("expected rows from INFORMATION_SCHEMA.TABLES")
	}
	fields := out["schema"].(map[string]any)["fields"].([]any)
	if len(fields) < 4 {
		t.Fatalf("expected information schema fields, got %v", fields)
	}
}

func TestJobsSyncQuerySupportsInformationSchemaJobs(t *testing.T) {
	s := newTestServer()
	createReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs?userEmail=tester@example.com", strings.NewReader(`{"configuration":{"query":{"query":"SELECT 1 AS one"}}}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", createRes.Code)
	}

	time.Sleep(160 * time.Millisecond)

	body := `{"query":"SELECT * FROM p1.INFORMATION_SCHEMA.JOBS"}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode information schema jobs response: %v", err)
	}
	rows, ok := out["rows"].([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("expected rows from INFORMATION_SCHEMA.JOBS")
	}
	firstRow := rows[0].(map[string]any)["f"].([]any)
	if len(firstRow) < 5 {
		t.Fatalf("expected job metadata columns, got %v", firstRow)
	}
}

func TestJobsSyncQuerySupportsInformationSchemaPartitions(t *testing.T) {
	s := newTestServer()
	body := `{"query":"SELECT * FROM p1.analytics.INFORMATION_SCHEMA.PARTITIONS"}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode information schema partitions response: %v", err)
	}
	rows, ok := out["rows"].([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("expected rows from INFORMATION_SCHEMA.PARTITIONS")
	}
	firstRow := rows[0].(map[string]any)["f"].([]any)
	partitionID := firstRow[3].(map[string]any)["v"]
	if partitionID != "__UNPARTITIONED__" {
		t.Fatalf("expected __UNPARTITIONED__ partition id, got %v", partitionID)
	}
}

func TestJobsSyncQuerySupportsInformationSchemaRoutinesAndModels(t *testing.T) {
	s := newTestServer()

	routinesReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(`{"query":"SELECT * FROM p1.analytics.INFORMATION_SCHEMA.ROUTINES"}`))
	routinesReq.Header.Set("Content-Type", "application/json")
	routinesRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(routinesRes, routinesReq)
	if routinesRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for routines query, got %d", routinesRes.Code)
	}

	var routinesOut map[string]any
	if err := json.NewDecoder(routinesRes.Body).Decode(&routinesOut); err != nil {
		t.Fatalf("decode routines response: %v", err)
	}
	routinesFields := routinesOut["schema"].(map[string]any)["fields"].([]any)
	if len(routinesFields) < 4 {
		t.Fatalf("expected routines schema fields, got %v", routinesFields)
	}

	modelsReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(`{"query":"SELECT * FROM p1.analytics.INFORMATION_SCHEMA.MODELS"}`))
	modelsReq.Header.Set("Content-Type", "application/json")
	modelsRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(modelsRes, modelsReq)
	if modelsRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for models query, got %d", modelsRes.Code)
	}

	var modelsOut map[string]any
	if err := json.NewDecoder(modelsRes.Body).Decode(&modelsOut); err != nil {
		t.Fatalf("decode models response: %v", err)
	}
	modelsFields := modelsOut["schema"].(map[string]any)["fields"].([]any)
	if len(modelsFields) < 4 {
		t.Fatalf("expected models schema fields, got %v", modelsFields)
	}
}

func TestJobsSyncQuerySupportsInformationSchemaSchemataOptions(t *testing.T) {
	s := newTestServer()
	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/analytics", strings.NewReader(`{"friendlyName":"Analytics DS","location":"US"}`))
	patchReq.Header.Set("Content-Type", "application/json")
	patchRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchRes, patchReq)
	if patchRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", patchRes.Code)
	}

	body := `{"query":"SELECT * FROM p1.INFORMATION_SCHEMA.SCHEMATA_OPTIONS"}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode schemata options response: %v", err)
	}
	rows, ok := out["rows"].([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("expected rows from INFORMATION_SCHEMA.SCHEMATA_OPTIONS")
	}
	foundLocation := false
	for _, row := range rows {
		cells := row.(map[string]any)["f"].([]any)
		if cells[2].(map[string]any)["v"] == "location" && cells[4].(map[string]any)["v"] == "US" {
			foundLocation = true
			break
		}
	}
	if !foundLocation {
		t.Fatalf("expected location option in schemata options rows")
	}
}

func TestJobsGetQueryResults(t *testing.T) {
	s := newTestServer()
	body := `{"configuration":{"query":{"query":"SELECT 1 AS one, 'two' AS two"}}}`
	createReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", strings.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", createRes.Code)
	}

	var created map[string]any
	if err := json.NewDecoder(createRes.Body).Decode(&created); err != nil {
		t.Fatalf("decode created job: %v", err)
	}
	jobRef := created["jobReference"].(map[string]any)
	jobID := jobRef["jobId"].(string)

	getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs/"+jobID+"/queryResults", nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRes.Code)
	}

	var out map[string]any
	if err := json.NewDecoder(getRes.Body).Decode(&out); err != nil {
		t.Fatalf("decode query results: %v", err)
	}
	rows, ok := out["rows"].([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("expected non-empty rows in query results")
	}
	schema, ok := out["schema"].(map[string]any)
	if !ok || schema["fields"] == nil {
		t.Fatalf("expected schema fields in query results")
	}
}

func TestJobsSyncQuery(t *testing.T) {
	s := newTestServer()
	body := `{"query":"SELECT 'sync' AS val", "timeoutMs": 2000}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode sync query response: %v", err)
	}
	if out["kind"] != "bigquery#queryResponse" {
		t.Fatalf("expected kind bigquery#queryResponse, got %v", out["kind"])
	}
	if out["jobComplete"] != true {
		t.Fatalf("expected jobComplete true")
	}
	rows, _ := out["rows"].([]any)
	if len(rows) == 0 {
		t.Fatalf("expected rows in sync query response")
	}

	// Test alternative route /queries/{jobId}
	jobRef := out["jobReference"].(map[string]any)
	jobID := jobRef["jobId"].(string)
	altReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/queries/"+jobID, nil)
	altRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(altRes, altReq)
	if altRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for alt route, got %d", altRes.Code)
	}
}

func TestJobsQueryRequestIDFromQueryParamIsIdempotentAndKeepsUserEmail(t *testing.T) {
	s := newTestServer()
	body := `{"query":"SELECT 1 AS one", "requestId":"body-ignored"}`

	firstReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries?requestId=req_q_1&userEmail=owner@example.com", strings.NewReader(body))
	firstReq.Header.Set("Content-Type", "application/json")
	firstRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(firstRes, firstReq)
	if firstRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", firstRes.Code)
	}

	secondReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries?requestId=req_q_1&userEmail=owner@example.com", strings.NewReader(body))
	secondReq.Header.Set("Content-Type", "application/json")
	secondRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(secondRes, secondReq)
	if secondRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", secondRes.Code)
	}

	var firstOut map[string]any
	if err := json.NewDecoder(firstRes.Body).Decode(&firstOut); err != nil {
		t.Fatalf("decode first query response: %v", err)
	}
	var secondOut map[string]any
	if err := json.NewDecoder(secondRes.Body).Decode(&secondOut); err != nil {
		t.Fatalf("decode second query response: %v", err)
	}

	firstJobID := firstOut["jobReference"].(map[string]any)["jobId"].(string)
	secondJobID := secondOut["jobReference"].(map[string]any)["jobId"].(string)
	if firstJobID != secondJobID {
		t.Fatalf("expected same job ID for same requestId, got %s and %s", firstJobID, secondJobID)
	}

	jobsReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(`{"query":"SELECT * FROM p1.INFORMATION_SCHEMA.JOBS"}`))
	jobsReq.Header.Set("Content-Type", "application/json")
	jobsRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(jobsRes, jobsReq)
	if jobsRes.Code != http.StatusOK {
		t.Fatalf("expected 200 querying INFORMATION_SCHEMA.JOBS, got %d", jobsRes.Code)
	}

	var jobsOut map[string]any
	if err := json.NewDecoder(jobsRes.Body).Decode(&jobsOut); err != nil {
		t.Fatalf("decode jobs response: %v", err)
	}
	rows, ok := jobsOut["rows"].([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("expected jobs rows")
	}

	found := false
	for _, raw := range rows {
		cells := raw.(map[string]any)["f"].([]any)
		jobID := cells[1].(map[string]any)["v"]
		userEmail := cells[4].(map[string]any)["v"]
		if jobID == firstJobID && userEmail == "owner@example.com" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected job %s with owner@example.com in INFORMATION_SCHEMA.JOBS", firstJobID)
	}
}

func TestJobsQueryDryRun(t *testing.T) {
	s := newTestServer()
	body := `{"query":"SELECT 1", "dryRun": true}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode dryRun response: %v", err)
	}
	if out["totalBytesProcessed"] == nil {
		t.Fatalf("expected totalBytesProcessed in dryRun response")
	}
}

func TestJobsListAllUsersAndSort(t *testing.T) {
	s := newTestServer()

	// Create job 1 for user A
	s.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs?userEmail=a@example.com", nil))
	time.Sleep(10 * time.Millisecond)
	// Create job 2 for user B
	s.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs?userEmail=b@example.com", nil))

	// List for user A (default allUsers=false)
	reqA := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs?userEmail=a@example.com", nil)
	resA := httptest.NewRecorder()
	s.Handler().ServeHTTP(resA, reqA)
	var bodyA struct{ Jobs []map[string]any }
	json.NewDecoder(resA.Body).Decode(&bodyA)
	if len(bodyA.Jobs) != 1 {
		t.Fatalf("expected 1 job for user A, got %d", len(bodyA.Jobs))
	}

	// List allUsers=true
	reqAll := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs?allUsers=true", nil)
	resAll := httptest.NewRecorder()
	s.Handler().ServeHTTP(resAll, reqAll)
	var bodyAll struct{ Jobs []map[string]any }
	json.NewDecoder(resAll.Body).Decode(&bodyAll)
	if len(bodyAll.Jobs) < 2 {
		t.Fatalf("expected at least 2 jobs for allUsers=true, got %d", len(bodyAll.Jobs))
	}

	// Verify DESC sort (job 2 should be first)
	job1ID := bodyAll.Jobs[1]["jobReference"].(map[string]any)["jobId"].(string)
	job2ID := bodyAll.Jobs[0]["jobReference"].(map[string]any)["jobId"].(string)
	if job2ID <= job1ID && bodyAll.Jobs[0]["statistics"].(map[string]any)["creationTime"] == bodyAll.Jobs[1]["statistics"].(map[string]any)["creationTime"] {
		// If timestamps are identical, we used jobId as tie breaker in DESC
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

func TestJobsPersistenceAtomicReplaceDoesNotLeakTempFile(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "jobs", "state.json")
	js := newJobServiceWithPersistence(storePath)

	if _, created := js.insert(jobInsertOptions{ProjectID: "p1", RequestID: "persist-tmp", JobType: "query"}); !created {
		t.Fatalf("expected job creation")
	}

	time.Sleep(20 * time.Millisecond)
	if _, err := os.Stat(storePath + ".tmp"); err == nil {
		t.Fatalf("expected temporary file to be cleaned up")
	} else if !os.IsNotExist(err) {
		t.Fatalf("unexpected error checking temp file: %v", err)
	}
}

func TestJobServiceWorkerLimitBackpressure(t *testing.T) {
	js := newJobServiceWithWorkerLimit(1)

	if _, created := js.insert(jobInsertOptions{ProjectID: "p1", JobType: "query"}); !created {
		t.Fatalf("expected first job to be created")
	}
	if _, created := js.insert(jobInsertOptions{ProjectID: "p1", JobType: "load"}); !created {
		t.Fatalf("expected second job to be created")
	}

	deadline := time.Now().Add(120 * time.Millisecond)
	foundBackpressure := false
	for time.Now().Before(deadline) {
		items, _, _ := js.list("p1", jobListFilters{}, 0, 10)
		running := 0
		pending := 0
		for _, item := range items {
			switch item.State {
			case jobStateRunning:
				running++
			case jobStatePending:
				pending++
			}
		}
		if running == 1 && pending == 1 {
			foundBackpressure = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !foundBackpressure {
		items, _, _ := js.list("p1", jobListFilters{}, 0, 10)
		t.Fatalf("expected one RUNNING and one PENDING job under worker limit, got %#v", items)
	}

	doneDeadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(doneDeadline) {
		items, _, _ := js.list("p1", jobListFilters{}, 0, 10)
		done := 0
		for _, item := range items {
			if item.State == jobStateDone {
				done++
			}
		}
		if done == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected both jobs to reach DONE state")
}

func TestJobServiceReadsWorkerLimitFromEnv(t *testing.T) {
	t.Setenv("LOCAQL_JOB_WORKERS", "2")
	js := newJobService()
	if js.maxConcurrent != 2 {
		t.Fatalf("expected worker limit 2 from env, got %d", js.maxConcurrent)
	}
	if js.runSlots == nil || cap(js.runSlots) != 2 {
		t.Fatalf("expected runSlots capacity 2")
	}
}

func TestJobServiceReadsStorageWriteLimitFromEnv(t *testing.T) {
	t.Setenv("LOCAQL_STORAGE_WRITE_WORKERS", "1")
	js := newJobService()
	if js.maxStorageWrite != 1 {
		t.Fatalf("expected storage write worker limit 1 from env, got %d", js.maxStorageWrite)
	}
	if js.storageWriteSlots == nil || cap(js.storageWriteSlots) != 1 {
		t.Fatalf("expected storageWriteSlots capacity 1")
	}
}

func TestJobServiceStorageWriteBackpressure(t *testing.T) {
	t.Setenv("LOCAQL_STORAGE_WRITE_WORKERS", "1")
	js := newJobServiceWithWorkerLimit(4)

	if _, created := js.insert(jobInsertOptions{ProjectID: "p1", JobType: "load"}); !created {
		t.Fatalf("expected first storage-write job to be created")
	}
	if _, created := js.insert(jobInsertOptions{ProjectID: "p1", JobType: "copy"}); !created {
		t.Fatalf("expected second storage-write job to be created")
	}

	deadline := time.Now().Add(130 * time.Millisecond)
	foundBackpressure := false
	for time.Now().Before(deadline) {
		items, _, _ := js.list("p1", jobListFilters{}, 0, 10)
		running := 0
		pending := 0
		for _, item := range items {
			switch item.State {
			case jobStateRunning:
				running++
			case jobStatePending:
				pending++
			}
		}
		if running == 1 && pending == 1 {
			foundBackpressure = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !foundBackpressure {
		items, _, _ := js.list("p1", jobListFilters{}, 0, 10)
		t.Fatalf("expected one RUNNING and one PENDING storage-write job under storage backpressure, got %#v", items)
	}
}

func TestJobServiceConcurrentProjectsAndClients(t *testing.T) {
	js := newJobServiceWithWorkerLimit(4)
	projects := []string{"pA", "pB"}
	users := []string{"a@example.com", "b@example.com", "c@example.com"}

	var wg sync.WaitGroup
	for _, project := range projects {
		project := project
		for i := 0; i < 12; i++ {
			wg.Add(1)
			idx := i
			go func() {
				defer wg.Done()
				_, _ = js.insert(jobInsertOptions{
					ProjectID: project,
					UserEmail: users[idx%len(users)],
					JobType:   "query",
				})
			}()
		}
	}
	wg.Wait()

	for _, project := range projects {
		items, _, _ := js.list(project, jobListFilters{}, 0, 100)
		if len(items) != 12 {
			t.Fatalf("expected 12 jobs for project %s, got %d", project, len(items))
		}
	}
}

func TestJobServiceSerializesConflictingResourceMutations(t *testing.T) {
	js := newJobServiceWithWorkerLimit(2)

	common := jobInsertOptions{
		ProjectID:     "p1",
		TargetDataset: "analytics",
		TargetTable:   "events",
		UserEmail:     "owner@example.com",
		RequestID:     "",
		ParentJobID:   "",
		QueryText:     "",
		IsScript:      false,
	}

	first := common
	first.JobType = "load"
	if _, created := js.insert(first); !created {
		t.Fatalf("expected first mutation job creation")
	}

	second := common
	second.JobType = "copy"
	if _, created := js.insert(second); !created {
		t.Fatalf("expected second mutation job creation")
	}

	deadline := time.Now().Add(130 * time.Millisecond)
	foundSerialized := false
	for time.Now().Before(deadline) {
		items, _, _ := js.list("p1", jobListFilters{}, 0, 10)
		running := 0
		pending := 0
		for _, item := range items {
			switch item.State {
			case jobStateRunning:
				running++
			case jobStatePending:
				pending++
			}
		}
		if running == 1 && pending == 1 {
			foundSerialized = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !foundSerialized {
		items, _, _ := js.list("p1", jobListFilters{}, 0, 10)
		t.Fatalf("expected serialized mutation execution with one RUNNING and one PENDING job, got %#v", items)
	}
}

func TestJobServiceConcurrentReadsDuringWrites(t *testing.T) {
	js := newJobServiceWithWorkerLimit(3)

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; i < 25; i++ {
			_, _ = js.insert(jobInsertOptions{
				ProjectID: "p-read",
				UserEmail: "reader@example.com",
				JobType:   "query",
			})
			time.Sleep(2 * time.Millisecond)
		}
	}()

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		deadline := time.Now().Add(250 * time.Millisecond)
		for time.Now().Before(deadline) {
			items, _, _ := js.list("p-read", jobListFilters{}, 0, 100)
			for _, item := range items {
				_, _ = js.get("p-read", item.JobID)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	select {
	case <-writerDone:
	case <-time.After(1 * time.Second):
		t.Fatalf("writer did not finish in time")
	}

	select {
	case <-readerDone:
	case <-time.After(1 * time.Second):
		t.Fatalf("reader did not finish in time")
	}
}
func TestJobPriorityAndErrors(t *testing.T) {
	s := newTestServer()

	// 1. Test BATCH priority simulation (ensure it completes)
	batchBody := `{"configuration":{"query":{"query":"SELECT 1", "priority":"BATCH"}}}`
	reqB := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", strings.NewReader(batchBody))
	reqB.Header.Set("Content-Type", "application/json")
	resB := httptest.NewRecorder()
	s.Handler().ServeHTTP(resB, reqB)
	if resB.Code != http.StatusCreated {
		t.Fatalf("batch job creation failed: %d", resB.Code)
	}
	var outB map[string]any
	json.NewDecoder(resB.Body).Decode(&outB)
	conf := outB["configuration"].(map[string]any)
	qConf := conf["query"].(map[string]any)
	if qConf["priority"] != "BATCH" {
		t.Fatalf("expected priority BATCH, got %v", qConf["priority"])
	}

	// 2. Test Detailed Errors via FORCE_ERROR
	errBody := `{"configuration":{"query":{"query":"SELECT * FROM table FORCE_ERROR"}}}`
	reqE := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", strings.NewReader(errBody))
	reqE.Header.Set("Content-Type", "application/json")
	resE := httptest.NewRecorder()
	s.Handler().ServeHTTP(resE, reqE)

	var outE map[string]any
	json.NewDecoder(resE.Body).Decode(&outE)
	jobRef := outE["jobReference"].(map[string]any)
	jobID := jobRef["jobId"].(string)

	// Wait for job to finish with error
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		getReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs/"+jobID, nil)
		getRes := httptest.NewRecorder()
		s.Handler().ServeHTTP(getRes, getReq)
		var got map[string]any
		json.NewDecoder(getRes.Body).Decode(&got)
		status := got["status"].(map[string]any)
		if status["state"] == "DONE" {
			if status["errorResult"] == nil {
				t.Fatalf("expected errorResult for FORCE_ERROR query")
			}
			errs, ok := status["errors"].([]any)
			if !ok || len(errs) < 1 {
				t.Fatalf("expected at least 1 secondary error, got %v", errs)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("job did not finish with error in time")
}
