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
