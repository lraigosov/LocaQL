package workspace

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildPlanIncludesWorkspaceFiles(t *testing.T) {
	root := tempWorkspace(t)
	writeWorkspaceFile(t, root, "queries/q.sql", "SELECT 1")
	writeWorkspaceFile(t, root, "schemas/users.json", "{}")

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("build plan failed: %v", err)
	}
	if !plan.Validation.IsValid() {
		t.Fatalf("expected valid plan, missing: %v", plan.Validation.MissingRequired)
	}
	if len(plan.Files) < 3 {
		t.Fatalf("expected file inventory, got: %d", len(plan.Files))
	}
}

func TestDiffDetectsAddedAndChangedFiles(t *testing.T) {
	source := tempWorkspace(t)
	target := tempWorkspace(t)

	writeWorkspaceFile(t, source, "queries/q.sql", "SELECT 1")
	writeWorkspaceFile(t, source, "schemas/users.json", "{\"a\":1}")
	writeWorkspaceFile(t, target, "schemas/users.json", "{\"a\":2}")

	d, err := Diff(source, target)
	if err != nil {
		t.Fatalf("diff failed: %v", err)
	}
	if len(d.OnlyInSource) == 0 {
		t.Fatalf("expected files only in source")
	}
	if len(d.Changed) == 0 {
		t.Fatalf("expected changed files")
	}
}

func TestBuildApplyDryRunActions(t *testing.T) {
	source := tempWorkspace(t)
	target := tempWorkspace(t)

	writeWorkspaceFile(t, source, "queries/new.sql", "SELECT 7")
	writeWorkspaceFile(t, source, "schemas/model.json", "{\"v\":1}")
	writeWorkspaceFile(t, target, "schemas/model.json", "{\"v\":0}")

	res, err := BuildApplyDryRun(source, target)
	if err != nil {
		t.Fatalf("build dry run failed: %v", err)
	}
	if len(res.Actions) == 0 {
		t.Fatalf("expected dry-run actions")
	}
}

func tempWorkspace(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	mustMkdir(t, filepath.Join(root, "datasets"))
	mustMkdir(t, filepath.Join(root, "schemas"))
	mustMkdir(t, filepath.Join(root, "queries"))
	mustMkdir(t, filepath.Join(root, "profiles"))
	mustWriteFile(t, filepath.Join(root, "manifest.yaml"), []byte("name: demo\n"))
	return root
}

func writeWorkspaceFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir for file %s: %v", full, err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", full, err)
	}
}
