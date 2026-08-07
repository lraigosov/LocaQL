package server

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBigQueryMultipartUploadIngestsFileData(t *testing.T) {
	s := newTestServer()
	metadata := uploadJobMetadata(t, "multipart_events", "NEWLINE_DELIMITED_JSON")
	data := []byte("{\"event_id\":1,\"event_name\":\"page_view\"}\n{\"event_id\":2,\"event_name\":\"checkout\"}\n")

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)
	metadataHeader := make(textproto.MIMEHeader)
	metadataHeader.Set("Content-Type", "application/json; charset=UTF-8")
	metadataPart, err := writer.CreatePart(metadataHeader)
	if err != nil {
		t.Fatalf("create metadata part: %v", err)
	}
	if _, err := metadataPart.Write(metadata); err != nil {
		t.Fatalf("write metadata part: %v", err)
	}
	mediaHeader := make(textproto.MIMEHeader)
	mediaHeader.Set("Content-Type", "application/octet-stream")
	mediaPart, err := writer.CreatePart(mediaHeader)
	if err != nil {
		t.Fatalf("create media part: %v", err)
	}
	if _, err := mediaPart.Write(data); err != nil {
		t.Fatalf("write media part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/upload/bigquery/v2/projects/p1/jobs?uploadType=multipart", &requestBody)
	req.Header.Set("Content-Type", "multipart/related; boundary="+writer.Boundary())
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", res.Code, res.Body.String())
	}

	jobID := assertUploadedLoadJobResponse(t, res, "multipart_events")
	waitForUploadedLoadJob(t, s, jobID, 2)
	assertUploadedPayloadReleased(t, s, jobID)
	assertUploadedTableRows(t, s, "multipart_events", "2", "1", "page_view")
}

func TestBigQueryResumableUploadAcceptsChunksAndIngestsFileData(t *testing.T) {
	s := newTestServer()
	metadata := uploadJobMetadata(t, "resumable_events", "CSV")
	data := []byte("event_id,event_name\n1,page_view\n2,checkout\n")

	initReq := httptest.NewRequest(http.MethodPost, "/upload/bigquery/v2/projects/p1/jobs?uploadType=resumable", bytes.NewReader(metadata))
	initReq.Header.Set("Content-Type", "application/json; charset=UTF-8")
	initReq.Header.Set("X-Upload-Content-Type", "application/octet-stream")
	initRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(initRes, initReq)
	if initRes.Code != http.StatusOK {
		t.Fatalf("expected resumable initiation 200, got %d: %s", initRes.Code, initRes.Body.String())
	}
	location := initRes.Header().Get("Location")
	if location == "" {
		t.Fatal("expected resumable upload Location header")
	}
	locationURL, err := url.Parse(location)
	if err != nil {
		t.Fatalf("parse upload location: %v", err)
	}

	firstEnd := 19
	firstReq := httptest.NewRequest(http.MethodPut, locationURL.RequestURI(), bytes.NewReader(data[:firstEnd]))
	firstReq.Header.Set("Content-Type", "application/octet-stream")
	firstReq.Header.Set("Content-Range", "bytes 0-18/*")
	firstRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(firstRes, firstReq)
	if firstRes.Code != http.StatusPermanentRedirect {
		t.Fatalf("expected 308 after intermediate chunk, got %d: %s", firstRes.Code, firstRes.Body.String())
	}
	if firstRes.Header().Get("Range") != "bytes=0-18" {
		t.Fatalf("unexpected committed range: %q", firstRes.Header().Get("Range"))
	}
	retryFirstReq := httptest.NewRequest(http.MethodPut, locationURL.RequestURI(), bytes.NewReader(data[:firstEnd]))
	retryFirstReq.Header.Set("Content-Type", "application/octet-stream")
	retryFirstReq.Header.Set("Content-Range", "bytes 0-18/*")
	retryFirstRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(retryFirstRes, retryFirstReq)
	if retryFirstRes.Code != http.StatusPermanentRedirect || retryFirstRes.Header().Get("Range") != "bytes=0-18" {
		t.Fatalf("expected idempotent retry to preserve committed range, got %d %q", retryFirstRes.Code, retryFirstRes.Header().Get("Range"))
	}

	finalReq := httptest.NewRequest(http.MethodPut, locationURL.RequestURI(), bytes.NewReader(data[firstEnd:]))
	finalReq.Header.Set("Content-Type", "application/octet-stream")
	finalReq.Header.Set("Content-Range", "bytes 19-"+strconvItoa(len(data)-1)+"/"+strconvItoa(len(data)))
	finalRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(finalRes, finalReq)
	if finalRes.Code != http.StatusOK {
		t.Fatalf("expected final upload 200, got %d: %s", finalRes.Code, finalRes.Body.String())
	}

	jobID := assertUploadedLoadJobResponse(t, finalRes, "resumable_events")
	retryFinalReq := httptest.NewRequest(http.MethodPut, locationURL.RequestURI(), bytes.NewReader(data[firstEnd:]))
	retryFinalReq.Header.Set("Content-Type", "application/octet-stream")
	retryFinalReq.Header.Set("Content-Range", "bytes 19-"+strconvItoa(len(data)-1)+"/"+strconvItoa(len(data)))
	retryFinalRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(retryFinalRes, retryFinalReq)
	if retryFinalRes.Code != http.StatusOK {
		t.Fatalf("expected completed upload retry 200, got %d: %s", retryFinalRes.Code, retryFinalRes.Body.String())
	}
	if retryJobID := assertUploadedLoadJobResponse(t, retryFinalRes, "resumable_events"); retryJobID != jobID {
		t.Fatalf("expected completed upload retry to return job %s, got %s", jobID, retryJobID)
	}
	waitForUploadedLoadJob(t, s, jobID, 2)
	assertUploadedPayloadReleased(t, s, jobID)
	assertUploadedTableRows(t, s, "resumable_events", "2", "1", "page_view")
}

func TestBigQueryResumableUploadAcceptsEmptyFile(t *testing.T) {
	s := newTestServer()
	metadata := uploadJobMetadata(t, "empty_upload", "CSV")
	initReq := httptest.NewRequest(http.MethodPost, "/upload/bigquery/v2/projects/p1/jobs?uploadType=resumable", bytes.NewReader(metadata))
	initRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(initRes, initReq)
	if initRes.Code != http.StatusOK {
		t.Fatalf("expected resumable initiation 200, got %d: %s", initRes.Code, initRes.Body.String())
	}
	locationURL, err := url.Parse(initRes.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse upload location: %v", err)
	}

	finalReq := httptest.NewRequest(http.MethodPut, locationURL.RequestURI(), http.NoBody)
	finalReq.Header.Set("Content-Range", "bytes */0")
	finalRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(finalRes, finalReq)
	if finalRes.Code != http.StatusOK {
		t.Fatalf("expected empty upload completion 200, got %d: %s", finalRes.Code, finalRes.Body.String())
	}
	jobID := assertUploadedLoadJobResponse(t, finalRes, "empty_upload")
	waitForUploadedLoadJob(t, s, jobID, 0)
	assertUploadedPayloadReleased(t, s, jobID)
	assertUploadedTableRows(t, s, "empty_upload", "0", "", "")
}

func TestBigQueryResumableUploadRejectsNonContiguousChunk(t *testing.T) {
	s := newTestServer()
	metadata := uploadJobMetadata(t, "bad_offset", "CSV")
	initReq := httptest.NewRequest(http.MethodPost, "/upload/bigquery/v2/projects/p1/jobs?uploadType=resumable", bytes.NewReader(metadata))
	initRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(initRes, initReq)
	locationURL, err := url.Parse(initRes.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse upload location: %v", err)
	}

	chunkReq := httptest.NewRequest(http.MethodPut, locationURL.RequestURI(), strings.NewReader("abc"))
	chunkReq.Header.Set("Content-Range", "bytes 5-7/8")
	chunkRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(chunkRes, chunkReq)
	if chunkRes.Code != http.StatusConflict {
		t.Fatalf("expected 409 for non-contiguous chunk, got %d: %s", chunkRes.Code, chunkRes.Body.String())
	}
}

func uploadJobMetadata(t *testing.T, tableID, sourceFormat string) []byte {
	t.Helper()
	payload := map[string]any{
		"configuration": map[string]any{
			"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": "analytics", "tableId": tableID},
				"schema": map[string]any{"fields": []any{
					map[string]any{"name": "event_id", "type": "INT64"},
					map[string]any{"name": "event_name", "type": "STRING"},
				}},
				"sourceFormat": sourceFormat,
				// Official clients encode BigQuery int64 fields as JSON strings.
				"skipLeadingRows":  "1",
				"writeDisposition": "WRITE_TRUNCATE",
			},
		},
	}
	if sourceFormat != "CSV" {
		payload["configuration"].(map[string]any)["load"].(map[string]any)["skipLeadingRows"] = "0"
	}
	metadata, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal upload metadata: %v", err)
	}
	return metadata
}

func assertUploadedLoadJobResponse(t *testing.T, res *httptest.ResponseRecorder, tableID string) string {
	t.Helper()
	var resource map[string]any
	if err := json.NewDecoder(res.Body).Decode(&resource); err != nil {
		t.Fatalf("decode uploaded job: %v", err)
	}
	configuration, ok := resource["configuration"].(map[string]any)
	if !ok {
		t.Fatalf("expected job configuration, got %v", resource["configuration"])
	}
	load, ok := configuration["load"].(map[string]any)
	if !ok {
		t.Fatalf("expected configuration.load, got %v", configuration)
	}
	destination := load["destinationTable"].(map[string]any)
	if destination["tableId"] != tableID {
		t.Fatalf("unexpected destination table: %v", destination)
	}
	return resource["jobReference"].(map[string]any)["jobId"].(string)
}

func waitForUploadedLoadJob(t *testing.T, s *Server, jobID string, expectedRows float64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs/"+jobID, nil)
		res := httptest.NewRecorder()
		s.Handler().ServeHTTP(res, req)
		var resource map[string]any
		if err := json.NewDecoder(res.Body).Decode(&resource); err != nil {
			t.Fatalf("decode load job: %v", err)
		}
		status := resource["status"].(map[string]any)
		if status["state"] == "DONE" {
			if status["errorResult"] != nil {
				t.Fatalf("unexpected load error: %v", status["errorResult"])
			}
			stats := resource["statistics"].(map[string]any)
			if stats["outputRows"] != expectedRows {
				t.Fatalf("expected %v rows, got %v", expectedRows, stats["outputRows"])
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish", jobID)
}

func assertUploadedTableRows(t *testing.T, s *Server, tableID, totalRows, firstID, firstName string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/tabledata/analytics/"+tableID+"/data", nil)
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected table data 200, got %d: %s", res.Code, res.Body.String())
	}
	var resource map[string]any
	if err := json.NewDecoder(res.Body).Decode(&resource); err != nil {
		t.Fatalf("decode table data: %v", err)
	}
	if resource["totalRows"] != totalRows {
		t.Fatalf("expected totalRows %s, got %v", totalRows, resource["totalRows"])
	}
	if totalRows == "0" {
		return
	}
	firstRow := resource["rows"].([]any)[0].(map[string]any)["f"].([]any)
	if firstRow[0].(map[string]any)["v"] != firstID || firstRow[1].(map[string]any)["v"] != firstName {
		t.Fatalf("unexpected first uploaded row: %v", firstRow)
	}
}

func assertUploadedPayloadReleased(t *testing.T, s *Server, jobID string) {
	t.Helper()
	job, ok := s.jobs.get("p1", jobID)
	if !ok {
		t.Fatalf("uploaded job %s not found", jobID)
	}
	if job.LoadInlineData != nil {
		t.Fatalf("expected uploaded job payload to be released after completion, retained %d bytes", len(job.LoadInlineData))
	}
}

func strconvItoa(value int) string {
	return strconv.FormatInt(int64(value), 10)
}
