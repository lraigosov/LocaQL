package workspace

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateWorkspaceRequiredStructure(t *testing.T) {
	root := t.TempDir()
	mustMkdir(t, filepath.Join(root, "datasets"))
	mustMkdir(t, filepath.Join(root, "schemas"))
	mustMkdir(t, filepath.Join(root, "queries"))
	mustMkdir(t, filepath.Join(root, "profiles"))
	mustWriteFile(t, filepath.Join(root, "manifest.yaml"), []byte("name: local\n"))

	res, err := Validate(root)
	if err != nil {
		t.Fatalf("validate failed: %v", err)
	}
	if !res.IsValid() {
		t.Fatalf("expected valid workspace, missing required: %v", res.MissingRequired)
	}
}

func TestValidateWorkspaceMissingRequired(t *testing.T) {
	root := t.TempDir()
	mustMkdir(t, filepath.Join(root, "datasets"))
	mustMkdir(t, filepath.Join(root, "schemas"))

	res, err := Validate(root)
	if err != nil {
		t.Fatalf("validate failed: %v", err)
	}
	if res.IsValid() {
		t.Fatalf("expected invalid workspace")
	}
	if len(res.MissingRequired) == 0 {
		t.Fatalf("expected missing required paths")
	}
}

func TestValidateWorkspacePathErrors(t *testing.T) {
	if _, err := Validate(""); err == nil {
		t.Fatalf("expected error for empty path")
	}

	root := t.TempDir()
	file := filepath.Join(root, "not_dir.txt")
	mustWriteFile(t, file, []byte("x"))
	if _, err := Validate(file); err == nil {
		t.Fatalf("expected error for non-directory path")
	}
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
