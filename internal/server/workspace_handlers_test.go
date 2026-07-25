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

func mustMkdirAll(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// newValidWorkspace creates a minimal but structurally valid portable
// workspace (all required paths present) under a fresh temp dir.
func newValidWorkspace(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "manifest.yaml"), "name: test-workspace\n")
	for _, dir := range []string{"datasets", "schemas", "queries", "profiles"} {
		mustMkdirAll(t, filepath.Join(root, dir))
	}
	mustWriteFile(t, filepath.Join(root, "datasets", "analytics.yaml"), "datasetId: analytics\n")
	return root
}

// newEmptyWorkspaceSkeleton creates a structurally valid workspace with no
// tracked files beyond manifest.yaml — the shape of a fresh apply target
// (workspace.BuildPlan requires manifest.yaml to exist, so an apply target
// must already be a scaffolded workspace, never a bare empty directory).
func newEmptyWorkspaceSkeleton(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "manifest.yaml"), "name: test-workspace\n")
	for _, dir := range []string{"datasets", "schemas", "queries", "profiles"} {
		mustMkdirAll(t, filepath.Join(root, dir))
	}
	return root
}

func postWorkspaceJSON(t *testing.T, s *Server, path string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	var out map[string]any
	if res.Body.Len() > 0 {
		if err := json.Unmarshal(res.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode response: %v (body: %s)", err, res.Body.String())
		}
	}
	return res.Code, out
}

func TestWorkspaceValidateReportsFoundAndMissing(t *testing.T) {
	s := newTestServer()
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "manifest.yaml"), "name: incomplete\n")
	mustMkdirAll(t, filepath.Join(root, "datasets"))
	// schemas, queries, profiles deliberately left missing.

	code, out := postWorkspaceJSON(t, s, "/_emulator/workspace/validate", map[string]any{"path": root})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if out["valid"] != false {
		t.Fatalf("expected valid=false with missing required paths, got %v", out["valid"])
	}
	missing, _ := out["missingRequired"].([]any)
	if len(missing) != 3 {
		t.Fatalf("expected 3 missing required paths (schemas, queries, profiles), got %v", missing)
	}
	found, _ := out["found"].([]any)
	if len(found) != 2 {
		t.Fatalf("expected 2 found paths (manifest.yaml, datasets), got %v", found)
	}
}

func TestWorkspaceValidateRejectsMissingPath(t *testing.T) {
	s := newTestServer()
	code, out := postWorkspaceJSON(t, s, "/_emulator/workspace/validate", map[string]any{"path": ""})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty path, got %d: %v", code, out)
	}
}

func TestWorkspacePlanListsRealFilesWithHashes(t *testing.T) {
	s := newTestServer()
	root := newValidWorkspace(t)

	code, out := postWorkspaceJSON(t, s, "/_emulator/workspace/plan", map[string]any{"path": root})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	validation, _ := out["validation"].(map[string]any)
	if validation["valid"] != true {
		t.Fatalf("expected a valid workspace, got %v", validation)
	}
	files, _ := out["files"].([]any)
	if len(files) != 2 {
		t.Fatalf("expected 2 tracked files (manifest.yaml, datasets/analytics.yaml), got %v", files)
	}
	first := files[0].(map[string]any)
	if first["sha256"] == "" || first["sha256"] == nil {
		t.Fatalf("expected a real sha256 hash, got %v", first)
	}
}

func TestWorkspaceDiffDetectsAddedChangedAndRemovedFiles(t *testing.T) {
	s := newTestServer()
	source := newValidWorkspace(t)
	target := t.TempDir()
	mustWriteFile(t, filepath.Join(target, "manifest.yaml"), "name: test-workspace\n")
	for _, dir := range []string{"datasets", "schemas", "queries", "profiles"} {
		mustMkdirAll(t, filepath.Join(target, dir))
	}
	// Target has a different version of the same file (should show as Changed)
	mustWriteFile(t, filepath.Join(target, "datasets", "analytics.yaml"), "datasetId: analytics\nstale: true\n")
	// Target has an extra file not in source (should show as OnlyInTarget)
	mustWriteFile(t, filepath.Join(target, "datasets", "extra.yaml"), "datasetId: extra\n")

	code, out := postWorkspaceJSON(t, s, "/_emulator/workspace/diff", map[string]any{"source": source, "target": target})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	changed, _ := out["changed"].([]any)
	if len(changed) != 1 {
		t.Fatalf("expected 1 changed file (datasets/analytics.yaml), got %v", changed)
	}
	onlyTarget, _ := out["onlyInTarget"].([]any)
	if len(onlyTarget) != 1 {
		t.Fatalf("expected 1 file only in target (datasets/extra.yaml), got %v", onlyTarget)
	}
}

func TestWorkspaceDiffRequiresTarget(t *testing.T) {
	s := newTestServer()
	code, out := postWorkspaceJSON(t, s, "/_emulator/workspace/diff", map[string]any{"source": "."})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing target, got %d: %v", code, out)
	}
}

func TestWorkspaceApplyDryRunDoesNotMutateTarget(t *testing.T) {
	s := newTestServer()
	source := newValidWorkspace(t)
	target := newEmptyWorkspaceSkeleton(t)

	code, out := postWorkspaceJSON(t, s, "/_emulator/workspace/apply", map[string]any{"source": source, "target": target})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if out["applied"] != false {
		t.Fatalf("expected applied=false for a dry run, got %v", out["applied"])
	}
	actions, _ := out["actions"].([]any)
	if len(actions) == 0 {
		t.Fatalf("expected planned actions in the dry run, got none")
	}
	if _, err := os.Stat(filepath.Join(target, "datasets", "analytics.yaml")); !os.IsNotExist(err) {
		t.Fatalf("expected dry run to leave target untouched, but datasets/analytics.yaml exists: err=%v", err)
	}
}

func TestWorkspaceApplyMutatingCopiesRealFiles(t *testing.T) {
	s := newTestServer()
	source := newValidWorkspace(t)
	target := newEmptyWorkspaceSkeleton(t)

	code, out := postWorkspaceJSON(t, s, "/_emulator/workspace/apply", map[string]any{
		"source": source, "target": target, "dryRun": false,
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if out["applied"] != true {
		t.Fatalf("expected applied=true, got %v", out["applied"])
	}
	content, err := os.ReadFile(filepath.Join(target, "datasets", "analytics.yaml"))
	if err != nil {
		t.Fatalf("expected the real file to be copied to target: %v", err)
	}
	if string(content) != "datasetId: analytics\n" {
		t.Fatalf("expected copied file content to match source, got %q", content)
	}
}

func TestWorkspaceApplyRequiresConfirmDeleteForMutatingDeleteMissing(t *testing.T) {
	s := newTestServer()
	source := newValidWorkspace(t)
	target := t.TempDir()
	mustWriteFile(t, filepath.Join(target, "datasets", "orphan.yaml"), "datasetId: orphan\n")

	code, out := postWorkspaceJSON(t, s, "/_emulator/workspace/apply", map[string]any{
		"source": source, "target": target, "dryRun": false, "deleteMissing": true,
	})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 without confirmDelete=DELETE, got %d: %v", code, out)
	}
	if _, err := os.Stat(filepath.Join(target, "datasets", "orphan.yaml")); err != nil {
		t.Fatalf("expected the orphan file to remain untouched after the rejected request: %v", err)
	}
}

func TestWorkspaceApplyDeleteMissingWithConfirmDeleteRemovesFiles(t *testing.T) {
	s := newTestServer()
	source := newValidWorkspace(t)
	target := newEmptyWorkspaceSkeleton(t)
	orphanPath := filepath.Join(target, "datasets", "orphan.yaml")
	mustWriteFile(t, orphanPath, "datasetId: orphan\n")

	code, out := postWorkspaceJSON(t, s, "/_emulator/workspace/apply", map[string]any{
		"source": source, "target": target, "dryRun": false,
		"deleteMissing": true, "confirmDelete": "DELETE",
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("expected orphan.yaml to be deleted, err=%v", err)
	}
}
