package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveLocalFilePathRejectsGCSWithoutFakeRoot(t *testing.T) {
	_, err := resolveLocalFilePath("gs://bucket/object.ndjson")
	if err == nil {
		t.Fatalf("expected error without LOCAQL_FAKE_GCS_ROOT configured")
	}
	if !strings.Contains(err.Error(), "LOCAQL_FAKE_GCS_ROOT") {
		t.Fatalf("expected error to mention LOCAQL_FAKE_GCS_ROOT as the escape hatch, got: %v", err)
	}
}

func TestResolveLocalFilePathMapsGCSToFakeRoot(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)

	path, err := resolveLocalFilePath("gs://mybucket/nested/events.ndjson")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := filepath.Join(root, "mybucket", "nested", "events.ndjson")
	if path != expected {
		t.Fatalf("expected %q, got %q", expected, path)
	}
}

func TestResolveLocalFilePathRejectsMissingBucket(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)

	_, err := resolveLocalFilePath("gs://")
	if err == nil || !strings.Contains(err.Error(), "bucket") {
		t.Fatalf("expected bucket-related error, got %v", err)
	}
}

func TestLoadJobIngestsNDJSONFromFakeGCSRoot(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)

	bucketDir := filepath.Join(root, "mybucket")
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		t.Fatalf("mkdir bucket dir: %v", err)
	}
	content := "{\"event_id\":1,\"event_name\":\"page_view\"}\n{\"event_id\":2,\"event_name\":\"checkout\"}\n"
	if err := os.WriteFile(filepath.Join(bucketDir, "events.ndjson"), []byte(content), 0o600); err != nil {
		t.Fatalf("write fake gcs fixture: %v", err)
	}

	s := newTestServer()
	bodyObj := map[string]any{
		"configuration": map[string]any{
			"load": map[string]any{
				"destinationTable": map[string]any{"projectId": "p1", "datasetId": "analytics", "tableId": "events_from_gcs"},
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
		t.Fatalf("marshal body: %v", err)
	}

	jobOut := runJobAndFetch(t, s, string(raw))
	status := jobOut["status"].(map[string]any)
	if status["errorResult"] != nil {
		t.Fatalf("unexpected job error: %v", status["errorResult"])
	}
	stats := jobOut["statistics"].(map[string]any)
	if stats["outputRows"] != float64(2) {
		t.Fatalf("expected 2 ingested rows from fake GCS root, got %v", stats["outputRows"])
	}
}

func TestExtractJobWritesToFakeGCSRoot(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOCAQL_FAKE_GCS_ROOT", root)

	s := newTestServer()
	body := `{"configuration":{"extract":{"sourceTable":{"projectId":"p1","datasetId":"analytics","tableId":"events"},"destinationUris":["gs://mybucket/out/events.csv"],"destinationFormat":"CSV"}}}`

	jobOut := runJobAndFetch(t, s, body)
	status := jobOut["status"].(map[string]any)
	if status["errorResult"] != nil {
		t.Fatalf("unexpected job error: %v", status["errorResult"])
	}

	expectedPath := filepath.Join(root, "mybucket", "out", "events.csv")
	data, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("expected extract output under fake GCS root at %q: %v", expectedPath, err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected header + 4 data lines, got %d: %q", len(lines), string(data))
	}
}
