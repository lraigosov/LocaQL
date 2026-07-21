package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunWorkspaceValidateSuccess(t *testing.T) {
	root := createWorkspaceRoot(t)
	if err := runWorkspace([]string{"validate", "--path", root}); err != nil {
		t.Fatalf("expected workspace validate success, got: %v", err)
	}
}

func TestRunWorkspaceValidateFailure(t *testing.T) {
	root := t.TempDir()
	mustMkdir(t, filepath.Join(root, "datasets"))
	if err := runWorkspace([]string{"validate", "--path", root}); err == nil {
		t.Fatalf("expected workspace validate failure")
	}
}

func TestRunWorkspacePlan(t *testing.T) {
	root := createWorkspaceRoot(t)
	mustWriteFile(t, filepath.Join(root, "queries", "q.sql"), []byte("SELECT 1"))
	if err := runWorkspace([]string{"plan", "--path", root}); err != nil {
		t.Fatalf("expected workspace plan success, got: %v", err)
	}
}

func TestRunWorkspaceDiff(t *testing.T) {
	source := createWorkspaceRoot(t)
	target := createWorkspaceRoot(t)
	mustWriteFile(t, filepath.Join(source, "queries", "q.sql"), []byte("SELECT 1"))
	mustWriteFile(t, filepath.Join(target, "queries", "q.sql"), []byte("SELECT 2"))

	if err := runWorkspace([]string{"diff", "--source", source, "--target", target}); err != nil {
		t.Fatalf("expected workspace diff success, got: %v", err)
	}
}

func TestRunWorkspaceApplyDryRun(t *testing.T) {
	source := createWorkspaceRoot(t)
	target := createWorkspaceRoot(t)
	mustWriteFile(t, filepath.Join(source, "queries", "q.sql"), []byte("SELECT 1"))

	if err := runWorkspace([]string{"apply", "--source", source, "--target", target, "--dry-run=true"}); err != nil {
		t.Fatalf("expected workspace apply dry-run success, got: %v", err)
	}
}

func TestRunWorkspaceApplyMutatingSuccess(t *testing.T) {
	source := createWorkspaceRoot(t)
	target := createWorkspaceRoot(t)
	mustWriteFile(t, filepath.Join(source, "queries", "q.sql"), []byte("SELECT 1"))

	if err := runWorkspace([]string{"apply", "--source", source, "--target", target, "--dry-run=false"}); err != nil {
		t.Fatalf("expected mutating apply success, got: %v", err)
	}

	if _, err := os.Stat(filepath.Join(target, "queries", "q.sql")); err != nil {
		t.Fatalf("expected target file after apply, got: %v", err)
	}
}

func TestRunWorkspaceApplyDeleteMissingRequiresConfirmation(t *testing.T) {
	source := createWorkspaceRoot(t)
	target := createWorkspaceRoot(t)
	mustWriteFile(t, filepath.Join(target, "queries", "legacy.sql"), []byte("SELECT 0"))

	if err := runWorkspace([]string{"apply", "--source", source, "--target", target, "--dry-run=false", "--delete-missing=true"}); err == nil {
		t.Fatalf("expected delete-missing confirmation error")
	}
}

func TestRunWorkspaceApplyDeleteMissingWithConfirmation(t *testing.T) {
	source := createWorkspaceRoot(t)
	target := createWorkspaceRoot(t)
	mustWriteFile(t, filepath.Join(target, "queries", "legacy.sql"), []byte("SELECT 0"))

	if err := runWorkspace([]string{"apply", "--source", source, "--target", target, "--dry-run=false", "--delete-missing=true", "--confirm-delete=DELETE"}); err != nil {
		t.Fatalf("expected confirmed delete-missing apply success, got: %v", err)
	}
	if _, err := os.Stat(filepath.Join(target, "queries", "legacy.sql")); !os.IsNotExist(err) {
		t.Fatalf("expected legacy.sql removed, got: %v", err)
	}
	
}

func TestRunWorkspaceApplyManifestOutput(t *testing.T) {
	source := createWorkspaceRoot(t)
	target := createWorkspaceRoot(t)
	mustWriteFile(t, filepath.Join(source, "queries", "q.sql"), []byte("SELECT 1"))
	manifestPath := filepath.Join(t.TempDir(), "apply-manifest.json")

	if err := runWorkspace([]string{"apply", "--source", source, "--target", target, "--dry-run=true", "--manifest-out", manifestPath}); err != nil {
		t.Fatalf("expected dry-run apply with manifest success, got: %v", err)
	}
	b, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("expected manifest file, got: %v", err)
	}
	if len(b) == 0 {
		t.Fatalf("expected non-empty manifest")
	}
}

func createWorkspaceRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	mustMkdir(t, filepath.Join(root, "datasets"))
	mustMkdir(t, filepath.Join(root, "schemas"))
	mustMkdir(t, filepath.Join(root, "queries"))
	mustMkdir(t, filepath.Join(root, "profiles"))
	mustWriteFile(t, filepath.Join(root, "manifest.yaml"), []byte("name: demo\n"))
	return root
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
}

func mustWriteFile(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
