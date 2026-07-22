//go:build e2e

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chromedp/chromedp"
)

func switchMainTab(target string) chromedp.Action {
	return chromedp.Click(`#mainTabs .tab[data-target="`+target+`"]`, chromedp.ByQuery)
}

func switchTableDetailsTab(target string) chromedp.Action {
	return chromedp.Click(`#tableDetailsTabs .tab[data-target="`+target+`"]`, chromedp.ByQuery)
}

// TestE2E_TableDetailsPreviewSchemaMetadata exercises
// console.ui.table_details.preview_schema_metadata: selecting a table opens
// metadata, schema, preview rows, labels editor and raw JSON details backed
// by tables.get and tabledata.list.
func TestE2E_TableDetailsPreviewSchemaMetadata(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx
	datasetID := nextID() + "_tdds"
	tableID := nextID() + "_tdtbl"

	ndjsonPath := filepath.Join(env.dir, "rows.ndjson")
	body := `{"id":1,"name":"foo"}` + "\n" + `{"id":2,"name":"bar"}` + "\n"
	if err := os.WriteFile(ndjsonPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write fixture ndjson: %v", err)
	}

	if err := run(ctx,
		setValue("newDatasetId", datasetID),
		submitForm("createDatasetForm"),
		pollTrue(treeContainsExpr(datasetID)),
		switchMainTab("load-extract-workspace"),
		setValue("loadDatasetId", datasetID),
		setValue("loadTableId", tableID),
		setValue("loadSchemaFields", `[{"name":"id","type":"INT64"},{"name":"name","type":"STRING"}]`),
		setValue("loadSourceUris", ndjsonPath),
		submitForm("loadJobForm"),
		pollTrue(`document.getElementById("loadJobStatus").textContent.includes("job submitted")`),
		switchMainTab("query-workspace"),
		pollTrue(treeContainsExpr(tableID)),
	); err != nil {
		t.Fatalf("seed table via load job: %v", err)
	}

	if err := run(ctx, chromedp.Evaluate(`(function(){
		const nodes = Array.from(document.querySelectorAll("#explorerTree .node.table"));
		const node = nodes.find(n => n.textContent.includes(`+jsString(tableID)+`));
		if (!node) throw new Error("table node not found: `+tableID+`");
		node.click();
	})()`, nil)); err != nil {
		t.Fatalf("select table in explorer: %v", err)
	}

	if err := run(ctx, pollTrue(`document.getElementById("tablePreviewTable").textContent.includes("foo")`)); err != nil {
		t.Fatalf("wait for load job rows to materialize: %v", err)
	}

	var tableName, tableType string
	if err := run(ctx,
		textOf("tableInfoName", &tableName),
		textOf("tableInfoType", &tableType),
	); err != nil {
		t.Fatalf("read overview: %v", err)
	}
	if tableType != "TABLE" {
		t.Errorf("expected Type TABLE, got %q", tableType)
	}
	if !strings.Contains(tableName, datasetID) || !strings.Contains(tableName, tableID) {
		t.Errorf("expected resolved table name to reference %s.%s, got %q", datasetID, tableID, tableName)
	}

	if err := run(ctx, switchTableDetailsTab("table-schema-view")); err != nil {
		t.Fatalf("open schema tab: %v", err)
	}
	var schemaText string
	if err := run(ctx, textOf("tableSchemaList", &schemaText)); err != nil {
		t.Fatalf("read schema list: %v", err)
	}
	if !strings.Contains(schemaText, "id") || !strings.Contains(schemaText, "name") {
		t.Errorf("expected schema list to show id/name fields, got %q", schemaText)
	}

	if err := run(ctx, switchTableDetailsTab("table-preview-view")); err != nil {
		t.Fatalf("open preview tab: %v", err)
	}
	var previewText string
	if err := run(ctx, textOf("tablePreviewTable", &previewText)); err != nil {
		t.Fatalf("read preview: %v", err)
	}
	if !strings.Contains(previewText, "foo") || !strings.Contains(previewText, "bar") {
		t.Errorf("expected preview rows foo/bar, got %q", previewText)
	}

	if err := run(ctx, switchTableDetailsTab("table-json-view")); err != nil {
		t.Fatalf("open json tab: %v", err)
	}
	var jsonText string
	if err := run(ctx, textOf("tableDetailsJson", &jsonText)); err != nil {
		t.Fatalf("read raw json: %v", err)
	}
	if !strings.Contains(jsonText, `"schema"`) {
		t.Errorf("expected raw JSON details to include schema, got %q", jsonText)
	}

	if err := run(ctx,
		switchTableDetailsTab("table-overview-view"),
		setValue("tableLabelsInput", `{"env":"e2e"}`),
		clickID("updateTableLabelsBtn"),
		pollTrue(`document.getElementById("tableInfoLabels").textContent.includes("e2e")`),
	); err != nil {
		t.Fatalf("edit labels: %v", err)
	}
}

// TestE2E_TableActionsQueryCopyDelete exercises
// console.ui.table_actions.query_copy_delete: the table details panel
// provides query draft, copy job submission and delete table actions
// through emulator endpoints.
func TestE2E_TableActionsQueryCopyDelete(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx
	datasetID := nextID() + "_tads"
	tableID := nextID() + "_tatbl"

	if err := run(ctx,
		setValue("newDatasetId", datasetID),
		submitForm("createDatasetForm"),
		pollTrue(treeContainsExpr(datasetID)),
		setValue("newTableDatasetId", datasetID),
		setValue("newTableId", tableID),
		submitForm("createTableForm"),
		pollTrue(treeContainsExpr(tableID)),
	); err != nil {
		t.Fatalf("setup dataset+table: %v", err)
	}

	// --- Query Table: drafts a SELECT and switches to the query workspace ---
	if err := run(ctx,
		clickID("queryTableBtn"),
		pollTrue(`document.getElementById("queryRunStatus").textContent.startsWith("query drafted for")`),
	); err != nil {
		t.Fatalf("query table action: %v", err)
	}
	var draftedSQL string
	if err := run(ctx, chromedp.Value(`#queryText`, &draftedSQL, chromedp.ByID)); err != nil {
		t.Fatalf("read drafted query: %v", err)
	}
	if !strings.Contains(draftedSQL, datasetID) || !strings.Contains(draftedSQL, tableID) {
		t.Errorf("expected drafted query to reference %s.%s, got %q", datasetID, tableID, draftedSQL)
	}
	// --- Copy Table: two window.prompt() dialogs accept their suggested
	// defaults (selectedDatasetId, then "<table>_copy"), submitting a real
	// copy job ---
	if err := run(ctx,
		clickID("copyTableBtn"),
		pollTrue(`document.getElementById("tableActionStatus").textContent.includes("copy job submitted")`),
	); err != nil {
		t.Fatalf("copy table action: %v", err)
	}
	var activeAfterCopy bool
	if err := run(ctx, evalBool(`document.querySelector("#mainTabs .tab[data-target=\"jobs-explorer\"]").classList.contains("active")`, &activeAfterCopy)); err != nil {
		t.Fatalf("check jobs tab active: %v", err)
	}
	if !activeAfterCopy {
		t.Error("expected copy table action to switch to the Jobs tab")
	}

	// --- Delete Table: confirm() dialog is auto-accepted. Switch back to the
	// query workspace first: the table details panel (and its Delete
	// button) is only visible under that tab. ---
	if err := run(ctx,
		switchMainTab("query-workspace"),
		waitVisibleSel(`#deleteTableBtn`),
		clickID("deleteTableBtn"),
		pollTrue(`!`+treeContainsExpr(tableID)),
	); err != nil {
		t.Fatalf("delete table action: %v", err)
	}
}
