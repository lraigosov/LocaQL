//go:build e2e

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestE2E_LoadExtractWorkspace exercises console.ui.load_extract_workspace:
// the dedicated Load / Extract tab submits real load jobs (CSV sourceUris,
// schema, delimiter/skip rows) and real extract jobs (destination format,
// delimiter, printHeader) through jobs.insert.
func TestE2E_LoadExtractWorkspace(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx
	datasetID := nextID() + "_leds"
	tableID := nextID() + "_letbl"

	csvPath := filepath.Join(env.dir, "load.csv")
	csvBody := "id,name\n1,alpha\n2,beta\n"
	if err := os.WriteFile(csvPath, []byte(csvBody), 0o644); err != nil {
		t.Fatalf("write load fixture: %v", err)
	}
	outPath := filepath.Join(env.dir, "extract-out.csv")

	if err := run(ctx,
		setValue("newDatasetId", datasetID),
		submitForm("createDatasetForm"),
		pollTrue(treeContainsExpr(datasetID)),
		switchMainTab("load-extract-workspace"),
	); err != nil {
		t.Fatalf("create dataset + open load/extract tab: %v", err)
	}

	if err := run(ctx,
		setValue("loadDatasetId", datasetID),
		setValue("loadTableId", tableID),
		setValue("loadSchemaFields", `[{"name":"id","type":"INT64"},{"name":"name","type":"STRING"}]`),
		setValue("loadSourceUris", csvPath),
		setValue("loadSourceFormat", "CSV"),
		setValue("loadFieldDelimiter", ","),
		setValue("loadSkipLeadingRows", "1"),
		submitForm("loadJobForm"),
		pollTrue(`document.getElementById("loadJobStatus").textContent.includes("job submitted")`),
	); err != nil {
		t.Fatalf("submit load job: %v", err)
	}
	var loadResult string
	if err := run(ctx, textOf("loadJobResult", &loadResult)); err != nil {
		t.Fatalf("read load job result: %v", err)
	}
	if !strings.Contains(loadResult, `"jobId"`) {
		t.Errorf("expected load job result JSON to include jobId, got %q", loadResult)
	}

	if err := run(ctx,
		setValue("extractDatasetId", datasetID),
		setValue("extractTableId", tableID),
		setValue("extractDestinationUris", outPath),
		setValue("extractDestinationFormat", "CSV"),
		submitForm("extractJobForm"),
		pollTrue(`document.getElementById("extractJobStatus").textContent.includes("job submitted")`),
	); err != nil {
		t.Fatalf("submit extract job: %v", err)
	}
	var extractResult string
	if err := run(ctx, textOf("extractJobResult", &extractResult)); err != nil {
		t.Fatalf("read extract job result: %v", err)
	}
	if !strings.Contains(extractResult, `"jobId"`) {
		t.Errorf("expected extract job result JSON to include jobId, got %q", extractResult)
	}

	waitForFileContent(t, outPath, "alpha", 10*time.Second)
	written, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if !strings.Contains(string(written), "alpha") || !strings.Contains(string(written), "beta") {
		t.Errorf("expected extracted CSV to contain the real table rows, got %q", string(written))
	}
}

func waitForFileContent(t *testing.T, path, substr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if body, err := os.ReadFile(path); err == nil && strings.Contains(string(body), substr) {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to contain %q", path, substr)
}
