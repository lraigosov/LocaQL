//go:build e2e

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestE2E_WorkspacePortability exercises console.ui.workspace_portability: the
// dedicated Workspace tab drives validate/plan/diff/apply against real
// on-disk workspace directories through the /_emulator/workspace/* REST
// endpoints, mirroring the locaql CLI's workspace subcommands.
func TestE2E_WorkspacePortability(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx

	source := filepath.Join(env.dir, "ws-source")
	target := filepath.Join(env.dir, "ws-target")
	for _, root := range []string{source, target} {
		for _, dir := range []string{"datasets", "schemas", "queries", "profiles"} {
			if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", dir, err)
			}
		}
		if err := os.WriteFile(filepath.Join(root, "manifest.yaml"), []byte("name: e2e-workspace\n"), 0o644); err != nil {
			t.Fatalf("write manifest.yaml: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "datasets", "analytics.yaml"), []byte("datasetId: analytics\n"), 0o644); err != nil {
		t.Fatalf("write source dataset file: %v", err)
	}

	if err := run(ctx, switchMainTab("workspace-portability")); err != nil {
		t.Fatalf("open workspace tab: %v", err)
	}

	// --- Validate: a structurally valid workspace reports valid=true. ---
	if err := run(ctx,
		setValue("workspaceValidatePath", source),
		submitForm("workspaceValidateForm"),
		pollTrue(`document.getElementById("workspaceValidateStatus").textContent === "valid"`),
	); err != nil {
		t.Fatalf("validate workspace: %v", err)
	}
	var validateResult string
	if err := run(ctx, textOf("workspaceValidateResult", &validateResult)); err != nil {
		t.Fatalf("read validate result: %v", err)
	}
	if !strings.Contains(validateResult, `"valid": true`) {
		t.Errorf("expected validate result to report valid: true, got %q", validateResult)
	}

	// --- Plan: the tracked file inventory includes the real dataset file. ---
	if err := run(ctx,
		setValue("workspacePlanPath", source),
		submitForm("workspacePlanForm"),
		pollTrue(`document.getElementById("workspacePlanStatus").textContent.includes("file(s) tracked")`),
	); err != nil {
		t.Fatalf("build plan: %v", err)
	}
	var planResult string
	if err := run(ctx, textOf("workspacePlanResult", &planResult)); err != nil {
		t.Fatalf("read plan result: %v", err)
	}
	if !strings.Contains(planResult, "datasets/analytics.yaml") {
		t.Errorf("expected plan result to list datasets/analytics.yaml, got %q", planResult)
	}

	// --- Diff: source has a file target doesn't. ---
	if err := run(ctx,
		setValue("workspaceDiffSource", source),
		setValue("workspaceDiffTarget", target),
		submitForm("workspaceDiffForm"),
		pollTrue(`document.getElementById("workspaceDiffStatus").textContent.includes("only-in-source")`),
	); err != nil {
		t.Fatalf("diff workspaces: %v", err)
	}
	var diffResult string
	if err := run(ctx, textOf("workspaceDiffResult", &diffResult)); err != nil {
		t.Fatalf("read diff result: %v", err)
	}
	if !strings.Contains(diffResult, "datasets/analytics.yaml") {
		t.Errorf("expected diff result to show datasets/analytics.yaml only in source, got %q", diffResult)
	}

	// --- Apply dry run (default): target must remain untouched on disk. ---
	if err := run(ctx,
		setValue("workspaceApplySource", source),
		setValue("workspaceApplyTarget", target),
		submitForm("workspaceApplyForm"),
		pollTrue(`document.getElementById("workspaceApplyStatus").textContent.includes("dry run")`),
	); err != nil {
		t.Fatalf("dry-run apply: %v", err)
	}
	if _, err := os.Stat(filepath.Join(target, "datasets", "analytics.yaml")); !os.IsNotExist(err) {
		t.Fatalf("expected dry run to leave target untouched, stat err: %v", err)
	}

	// --- Apply mutating: unchecking dryRun really copies the file to target. ---
	if err := run(ctx,
		setChecked("workspaceApplyDryRun", false),
		submitForm("workspaceApplyForm"),
		pollTrue(`document.getElementById("workspaceApplyStatus").textContent === "applied"`),
	); err != nil {
		t.Fatalf("mutating apply: %v", err)
	}
	copied, err := os.ReadFile(filepath.Join(target, "datasets", "analytics.yaml"))
	if err != nil {
		t.Fatalf("expected the real file to be copied to target: %v", err)
	}
	if string(copied) != "datasetId: analytics\n" {
		t.Errorf("expected copied file content to match source, got %q", string(copied))
	}
}
