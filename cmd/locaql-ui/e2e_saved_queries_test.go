//go:build e2e

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/chromedp"
)

// TestE2E_SavedQueriesVersioning exercises console.ui.saved_queries.versioning:
// re-saving under the same name keeps local version history, and Prev/Next
// navigate + Load restores the SQL text of the selected version.
func TestE2E_SavedQueriesVersioning(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx
	name := nextID() + "_verq"

	if err := run(ctx,
		setValue("savedQueryName", name),
		setValue("queryText", "SELECT 1"),
		submitForm("saveQueryForm"),
		pollTrue(`document.getElementById("savedQueriesList").textContent.includes(`+jsString(name)+`)`),
	); err != nil {
		t.Fatalf("save v1: %v", err)
	}

	if err := run(ctx,
		setValue("savedQueryName", name),
		setValue("queryText", "SELECT 2"),
		submitForm("saveQueryForm"),
		pollTrue(`(`+savedQueryLabelExpr(name)+`).includes("v2/2")`),
	); err != nil {
		t.Fatalf("save v2 (same name): %v", err)
	}

	if err := run(ctx,
		clickSavedQueryButton(name, "Prev"),
		pollTrue(`(`+savedQueryLabelExpr(name)+`).includes("v1/2")`),
		clickSavedQueryButton(name, "Load"),
	); err != nil {
		t.Fatalf("navigate to v1 and load: %v", err)
	}

	var loadedSQL string
	if err := run(ctx, chromedp.Value(`#queryText`, &loadedSQL, chromedp.ByID)); err != nil {
		t.Fatalf("read queryText after load: %v", err)
	}
	if loadedSQL != "SELECT 1" {
		t.Errorf("expected Prev+Load to restore v1 sql 'SELECT 1', got %q", loadedSQL)
	}

	if err := run(ctx,
		clickSavedQueryButton(name, "Next"),
		pollTrue(`(`+savedQueryLabelExpr(name)+`).includes("v2/2")`),
		clickSavedQueryButton(name, "Load"),
	); err != nil {
		t.Fatalf("navigate to v2 and load: %v", err)
	}
	if err := run(ctx, chromedp.Value(`#queryText`, &loadedSQL, chromedp.ByID)); err != nil {
		t.Fatalf("read queryText after Next+load: %v", err)
	}
	if loadedSQL != "SELECT 2" {
		t.Errorf("expected Next+Load to restore v2 sql 'SELECT 2', got %q", loadedSQL)
	}
}

// TestE2E_SavedQueriesImportExport exercises console.ui.saved_queries.import_export:
// Export downloads the saved-queries JSON, and Import merges a JSON file back in.
func TestE2E_SavedQueriesImportExport(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx
	exportName := nextID() + "_expq"

	if err := run(ctx,
		browser.SetDownloadBehavior(browser.SetDownloadBehaviorBehaviorAllow).WithDownloadPath(env.dir),
		setValue("savedQueryName", exportName),
		setValue("queryText", "SELECT 42"),
		submitForm("saveQueryForm"),
		pollTrue(`document.getElementById("savedQueriesList").textContent.includes(`+jsString(exportName)+`)`),
		clickID("exportSavedQueriesBtn"),
	); err != nil {
		t.Fatalf("save + export: %v", err)
	}

	downloaded := waitForDownload(t, env.dir, "locaql-saved-queries-", 10*time.Second)
	body, err := os.ReadFile(downloaded)
	if err != nil {
		t.Fatalf("read downloaded export: %v", err)
	}
	if !strings.Contains(string(body), exportName) || !strings.Contains(string(body), "SELECT 42") {
		t.Errorf("expected exported JSON to contain saved query %q and its sql, got %q", exportName, string(body))
	}

	// Import a fixture back in and confirm it merges into the visible list.
	importName := nextID() + "_impq"
	fixturePath := filepath.Join(env.dir, "import-fixture.json")
	fixture := `[{"name":"` + importName + `","versions":[{"sql":"SELECT 7","savedAt":1000}],"currentVersion":0}]`
	if err := os.WriteFile(fixturePath, []byte(fixture), 0o644); err != nil {
		t.Fatalf("write import fixture: %v", err)
	}

	if err := run(ctx,
		chromedp.SetUploadFiles(`#savedQueriesImportInput`, []string{fixturePath}, chromedp.ByID),
		pollTrue(`document.getElementById("savedQueriesList").textContent.includes(`+jsString(importName)+`)`),
	); err != nil {
		t.Fatalf("import fixture file: %v", err)
	}
}

// TestE2E_SavedQueriesShareLink exercises console.ui.saved_queries.share_link:
// Share copies a URL with the query text in a `query` search param.
func TestE2E_SavedQueriesShareLink(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx
	name := nextID() + "_shareq"
	sql := "SELECT 99"

	if err := run(ctx,
		setValue("savedQueryName", name),
		setValue("queryText", sql),
		submitForm("saveQueryForm"),
		pollTrue(`document.getElementById("savedQueriesList").textContent.includes(`+jsString(name)+`)`),
		clickSavedQueryButton(name, "Share"),
		pollTrue(`document.getElementById("queryRunStatus").textContent === "query link copied"`),
	); err != nil {
		t.Fatalf("share saved query: %v", err)
	}

	var clipboardText string
	if err := run(ctx, chromedp.Evaluate(`navigator.clipboard.readText()`, &clipboardText,
		func(p *runtime.EvaluateParams) *runtime.EvaluateParams {
			return p.WithAwaitPromise(true)
		},
	)); err != nil {
		t.Fatalf("read clipboard: %v", err)
	}
	if !strings.Contains(clipboardText, "query=") || !strings.Contains(clipboardText, strconv.Itoa(99)) {
		t.Errorf("expected clipboard share link to encode the query text, got %q", clipboardText)
	}
}

func waitForDownload(t *testing.T, dir, prefix string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dir)
		if err == nil {
			for _, e := range entries {
				if strings.HasPrefix(e.Name(), prefix) && !strings.HasSuffix(e.Name(), ".crdownload") {
					return filepath.Join(dir, e.Name())
				}
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for download with prefix %q in %s", prefix, dir)
	return ""
}
