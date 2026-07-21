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

func TestRunWorkspaceApplyNonDryRunRejected(t *testing.T) {
	source := createWorkspaceRoot(t)
	target := createWorkspaceRoot(t)
	if err := runWorkspace([]string{"apply", "--source", source, "--target", target, "--dry-run=false"}); err == nil {
		t.Fatalf("expected non dry-run apply rejection")
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
