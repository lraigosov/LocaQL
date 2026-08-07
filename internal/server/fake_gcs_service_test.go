package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGCSBucketsInsertAndList(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)
	s := newTestServer()

	insertReq := httptest.NewRequest(http.MethodPost, "/storage/v1/b", strings.NewReader(`{"name":"mybucket"}`))
	insertRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(insertRes, insertReq)
	if insertRes.Code != http.StatusOK {
		t.Fatalf("expected 200 creating bucket, got %d: %s", insertRes.Code, insertRes.Body.String())
	}
	var inserted map[string]any
	if err := json.NewDecoder(insertRes.Body).Decode(&inserted); err != nil {
		t.Fatalf("decode bucket insert: %v", err)
	}
	if inserted["name"] != "mybucket" || inserted["kind"] != "storage#bucket" {
		t.Fatalf("unexpected bucket resource: %v", inserted)
	}
	if _, err := os.Stat(filepath.Join(root, "mybucket")); err != nil {
		t.Fatalf("expected bucket directory created on disk: %v", err)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/storage/v1/b", nil)
	listRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(listRes, listReq)
	var listed map[string]any
	if err := json.NewDecoder(listRes.Body).Decode(&listed); err != nil {
		t.Fatalf("decode bucket list: %v", err)
	}
	items, ok := listed["items"].([]any)
	if !ok || len(items) != 1 || items[0].(map[string]any)["name"] != "mybucket" {
		t.Fatalf("expected 1 listed bucket named mybucket, got %v", listed["items"])
	}
}

func TestGCSObjectUploadGetDownloadDelete(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)
	s := newTestServer()

	uploadReq := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/mybucket/o?uploadType=media&name=events.csv", strings.NewReader("id,name\n1,alpha\n"))
	uploadRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(uploadRes, uploadReq)
	if uploadRes.Code != http.StatusOK {
		t.Fatalf("expected 200 uploading object, got %d: %s", uploadRes.Code, uploadRes.Body.String())
	}
	var uploaded map[string]any
	if err := json.NewDecoder(uploadRes.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}
	if uploaded["name"] != "events.csv" || uploaded["bucket"] != "mybucket" || uploaded["size"] != "16" {
		t.Fatalf("unexpected object resource: %v", uploaded)
	}
	if uploaded["mediaLink"] != "http://example.com/download/storage/v1/b/mybucket/o/events.csv?alt=media" {
		t.Fatalf("expected official absolute download mediaLink, got %v", uploaded["mediaLink"])
	}

	// Metadata get.
	metaReq := httptest.NewRequest(http.MethodGet, "/storage/v1/b/mybucket/o/events.csv", nil)
	metaRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(metaRes, metaReq)
	if metaRes.Code != http.StatusOK {
		t.Fatalf("expected 200 getting object metadata, got %d: %s", metaRes.Code, metaRes.Body.String())
	}

	// Content download.
	downloadReq := httptest.NewRequest(http.MethodGet, "/storage/v1/b/mybucket/o/events.csv?alt=media", nil)
	downloadRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(downloadRes, downloadReq)
	if downloadRes.Code != http.StatusOK {
		t.Fatalf("expected 200 downloading object, got %d", downloadRes.Code)
	}
	if downloadRes.Body.String() != "id,name\n1,alpha\n" {
		t.Fatalf("expected downloaded content to match uploaded bytes, got %q", downloadRes.Body.String())
	}

	// Delete, then confirm gone.
	deleteReq := httptest.NewRequest(http.MethodDelete, "/storage/v1/b/mybucket/o/events.csv", nil)
	deleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(deleteRes, deleteReq)
	if deleteRes.Code != http.StatusNoContent {
		t.Fatalf("expected 204 deleting object, got %d: %s", deleteRes.Code, deleteRes.Body.String())
	}
	getAfterDeleteReq := httptest.NewRequest(http.MethodGet, "/storage/v1/b/mybucket/o/events.csv", nil)
	getAfterDeleteRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getAfterDeleteRes, getAfterDeleteReq)
	if getAfterDeleteRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", getAfterDeleteRes.Code)
	}
}

func TestGCSOfficialDownloadPathSupportsEncodedNamesRangesAndHead(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)
	s := newTestServer()
	payload := "0123456789abcdef"

	uploadReq := httptest.NewRequest(
		http.MethodPost,
		"/upload/storage/v1/b/mybucket/o?uploadType=media&name=folder%2Fnested%20report.txt",
		strings.NewReader(payload),
	)
	uploadRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(uploadRes, uploadReq)
	if uploadRes.Code != http.StatusOK {
		t.Fatalf("expected 200 uploading encoded object name, got %d: %s", uploadRes.Code, uploadRes.Body.String())
	}
	var uploaded map[string]any
	if err := json.NewDecoder(uploadRes.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}
	wantMediaLink := "http://example.com/download/storage/v1/b/mybucket/o/folder%2Fnested%20report.txt?alt=media"
	if uploaded["mediaLink"] != wantMediaLink {
		t.Fatalf("expected encoded official mediaLink %q, got %v", wantMediaLink, uploaded["mediaLink"])
	}

	downloadPath := "/download/storage/v1/b/mybucket/o/folder%2Fnested%20report.txt?alt=media"
	downloadReq := httptest.NewRequest(http.MethodGet, downloadPath, nil)
	downloadRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(downloadRes, downloadReq)
	if downloadRes.Code != http.StatusOK || downloadRes.Body.String() != payload {
		t.Fatalf("expected full official-path download, got %d: %q", downloadRes.Code, downloadRes.Body.String())
	}
	if downloadRes.Header().Get("Accept-Ranges") != "bytes" || !strings.HasPrefix(downloadRes.Header().Get("X-Goog-Hash"), "md5=") {
		t.Fatalf("expected range and checksum headers, got %v", downloadRes.Header())
	}

	rangeReq := httptest.NewRequest(http.MethodGet, downloadPath, nil)
	rangeReq.Header.Set("Range", "bytes=4-9")
	rangeRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(rangeRes, rangeReq)
	if rangeRes.Code != http.StatusPartialContent || rangeRes.Body.String() != "456789" {
		t.Fatalf("expected 206 bytes 4-9, got %d: %q", rangeRes.Code, rangeRes.Body.String())
	}
	if got := rangeRes.Header().Get("Content-Range"); got != "bytes 4-9/16" {
		t.Fatalf("expected Content-Range bytes 4-9/16, got %q", got)
	}

	headReq := httptest.NewRequest(http.MethodHead, downloadPath, nil)
	headRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(headRes, headReq)
	if headRes.Code != http.StatusOK || headRes.Body.Len() != 0 || headRes.Header().Get("Content-Length") != "16" {
		t.Fatalf("expected HEAD 200 with no body and length 16, got %d headers=%v body=%q", headRes.Code, headRes.Header(), headRes.Body.String())
	}
}

func TestGCSOfficialDownloadPathErrorsUseStorageContract(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)
	s := newTestServer()

	missingReq := httptest.NewRequest(http.MethodGet, "/download/storage/v1/b/mybucket/o/missing.txt?alt=media", nil)
	missingRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(missingRes, missingReq)
	if missingRes.Code != http.StatusNotFound || !strings.Contains(missingRes.Body.String(), `"error"`) {
		t.Fatalf("expected GCS-shaped 404, got %d: %s", missingRes.Code, missingRes.Body.String())
	}

	postReq := httptest.NewRequest(http.MethodPost, "/download/storage/v1/b/mybucket/o/missing.txt?alt=media", nil)
	postRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(postRes, postReq)
	if postRes.Code != http.StatusMethodNotAllowed || !strings.Contains(postRes.Body.String(), `"error"`) {
		t.Fatalf("expected GCS-shaped 405, got %d: %s", postRes.Code, postRes.Body.String())
	}
}

func TestGCSObjectNameWithSlashesActsAsPseudoDirectory(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)
	s := newTestServer()

	uploadReq := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/mybucket/o?uploadType=media&name=folder/nested/events.ndjson", strings.NewReader(`{"id":1}`+"\n"))
	uploadRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(uploadRes, uploadReq)
	if uploadRes.Code != http.StatusOK {
		t.Fatalf("expected 200 uploading nested object, got %d: %s", uploadRes.Code, uploadRes.Body.String())
	}

	if _, err := os.Stat(filepath.Join(root, "mybucket", "folder", "nested", "events.ndjson")); err != nil {
		t.Fatalf("expected nested object materialized as nested path on disk: %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/storage/v1/b/mybucket/o/folder/nested/events.ndjson?alt=media", nil)
	getRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusOK || getRes.Body.String() != `{"id":1}`+"\n" {
		t.Fatalf("expected to read back nested object content, got %d: %q", getRes.Code, getRes.Body.String())
	}
}

func TestGCSListObjectsWithPrefix(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)
	s := newTestServer()

	for _, name := range []string{"logs/a.ndjson", "logs/b.ndjson", "other.csv"} {
		req := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/mybucket/o?uploadType=media&name="+name, strings.NewReader("x"))
		s.Handler().ServeHTTP(httptest.NewRecorder(), req)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/storage/v1/b/mybucket/o?prefix=logs/", nil)
	listRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(listRes, listReq)
	var listed map[string]any
	if err := json.NewDecoder(listRes.Body).Decode(&listed); err != nil {
		t.Fatalf("decode object list: %v", err)
	}
	items, ok := listed["items"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected 2 objects matching prefix logs/, got %v", listed["items"])
	}
}

func TestGCSAndLoadExtractGSURIsInteroperate(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)
	s := newTestServer()

	// Upload via the fake-GCS HTTP API...
	uploadReq := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/mybucket/o?uploadType=media&name=events.ndjson", strings.NewReader(`{"event_id":1,"event_name":"page_view"}`+"\n"))
	uploadRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(uploadRes, uploadReq)
	if uploadRes.Code != http.StatusOK {
		t.Fatalf("expected 200 uploading via fake-GCS API, got %d: %s", uploadRes.Code, uploadRes.Body.String())
	}

	// ...and load it via the existing gs:// sourceUris path.
	bodyObj := map[string]any{
		"configuration": map[string]any{
			"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": "analytics", "tableId": "events_via_gcs_api"},
				"schema": map[string]any{"fields": []any{
					map[string]any{"name": "event_id", "type": "INT64"},
					map[string]any{"name": "event_name", "type": "STRING"},
				}},
				"sourceUris":       []any{"gs://mybucket/events.ndjson"},
				"sourceFormat":     "NEWLINE_DELIMITED_JSON",
				"writeDisposition": "WRITE_TRUNCATE",
			},
		},
	}
	raw, err := json.Marshal(bodyObj)
	if err != nil {
		t.Fatalf("marshal load body: %v", err)
	}
	jobOut := runJobAndFetch(t, s, string(raw))
	status := jobOut["status"].(map[string]any)
	if status["errorResult"] != nil {
		t.Fatalf("unexpected load error: %v", status["errorResult"])
	}
	stats := jobOut["statistics"].(map[string]any)
	if stats["outputRows"] != float64(1) {
		t.Fatalf("expected 1 row loaded from the object uploaded via fake-GCS API, got %v", stats["outputRows"])
	}
}

// TestLoadJobStatisticsNestUnderJobType guards against a regression where a
// load job's row/byte counts were only ever written to the flat
// statistics.outputRows/processedBytes keys. Real BigQuery (and the
// google-cloud-bigquery client's LoadJob.output_rows/output_bytes
// properties, which read statistics.load.outputRows/outputBytes
// specifically) expects them nested under a job-type-keyed object instead —
// without that nesting, a client polling a load job it submitted always saw
// output_rows as None even though the emulator had computed the real count
// server-side. This also exercises numRows/numBytes on the resulting table,
// which were never set at all before (Table.num_rows was always None
// regardless of whether a load had populated the table).
func TestLoadJobStatisticsNestUnderJobType(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)
	s := newTestServer()

	uploadReq := httptest.NewRequest(
		http.MethodPost,
		"/upload/storage/v1/b/mybucket/o?uploadType=media&name=events2.ndjson",
		strings.NewReader(`{"event_id":1,"event_name":"page_view"}`+"\n"+`{"event_id":2,"event_name":"click"}`+"\n"),
	)
	uploadRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(uploadRes, uploadReq)
	if uploadRes.Code != http.StatusOK {
		t.Fatalf("expected 200 uploading via fake-GCS API, got %d: %s", uploadRes.Code, uploadRes.Body.String())
	}

	bodyObj := map[string]any{
		"configuration": map[string]any{
			"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": "analytics", "tableId": "events_stats_check"},
				"schema": map[string]any{"fields": []any{
					map[string]any{"name": "event_id", "type": "INT64"},
					map[string]any{"name": "event_name", "type": "STRING"},
				}},
				"sourceUris":       []any{"gs://mybucket/events2.ndjson"},
				"sourceFormat":     "NEWLINE_DELIMITED_JSON",
				"writeDisposition": "WRITE_TRUNCATE",
			},
		},
	}
	raw, err := json.Marshal(bodyObj)
	if err != nil {
		t.Fatalf("marshal load body: %v", err)
	}
	jobOut := runJobAndFetch(t, s, string(raw))
	stats := jobOut["statistics"].(map[string]any)

	loadStats, ok := stats["load"].(map[string]any)
	if !ok {
		t.Fatalf("expected statistics.load object, got statistics=%v", stats)
	}
	if loadStats["outputRows"] != float64(2) {
		t.Fatalf("expected statistics.load.outputRows=2, got %v", loadStats["outputRows"])
	}
	if _, ok := loadStats["outputBytes"]; !ok {
		t.Fatalf("expected statistics.load.outputBytes to be present, got %v", loadStats)
	}

	tableReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/datasets/analytics/tables/events_stats_check", nil)
	tableRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(tableRes, tableReq)
	if tableRes.Code != http.StatusOK {
		t.Fatalf("expected 200 fetching loaded table, got %d: %s", tableRes.Code, tableRes.Body.String())
	}
	var tableBody map[string]any
	if err := json.NewDecoder(tableRes.Body).Decode(&tableBody); err != nil {
		t.Fatalf("decode table: %v", err)
	}
	if tableBody["numRows"] != "2" {
		t.Fatalf("expected numRows=\"2\", got %v", tableBody["numRows"])
	}
}

func TestGCSRequiresFakeGCSRootConfigured(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b", nil)
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 without LOCAQL_FAKE_GCS_ROOT, got %d", res.Code)
	}
	if !strings.Contains(res.Body.String(), "LOCAQL_FAKE_GCS_ROOT") {
		t.Fatalf("expected error to mention LOCAQL_FAKE_GCS_ROOT, got %s", res.Body.String())
	}
}

func TestGCSUploadRejectsUnsupportedUploadType(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)
	s := newTestServer()

	req := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/mybucket/o?uploadType=resumable&name=big.csv", strings.NewReader("x"))
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 for unsupported uploadType, got %d: %s", res.Code, res.Body.String())
	}
}
