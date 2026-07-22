//go:build e2e

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chromedp/chromedp"
)

// TestE2E_ExternalTablesForm exercises the console.ui.external_tables
// capability: the dedicated Explorer form creates an external table against
// tables.insert's externalDataConfiguration, the details panel shows Type
// EXTERNAL plus the raw config block, the Explorer tree marks the node with
// an "(external)" suffix, and Preview reads live file contents through the
// same backend endpoint a direct API call would use.
func TestE2E_ExternalTablesForm(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx
	datasetID := nextID() + "_extds"
	tableID := nextID() + "_exttbl"

	csvPath := filepath.Join(env.dir, "events.csv")
	csvBody := "id,name\n1,alpha\n2,beta\n"
	if err := os.WriteFile(csvPath, []byte(csvBody), 0o644); err != nil {
		t.Fatalf("write fixture csv: %v", err)
	}

	if err := run(ctx,
		setValue("newDatasetId", datasetID),
		submitForm("createDatasetForm"),
		pollTrue(treeContainsExpr(datasetID)),
	); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	if err := run(ctx,
		setValue("newExternalTableDatasetId", datasetID),
		setValue("newExternalTableId", tableID),
		setValue("newExternalTableSchemaFields", `[{"name":"id","type":"INT64"},{"name":"name","type":"STRING"}]`),
		setValue("newExternalTableSourceUris", csvPath),
		setValue("newExternalTableSourceFormat", "CSV"),
		setValue("newExternalTableSkipLeadingRows", "1"),
		submitForm("createExternalTableForm"),
		pollTrue(treeContainsExpr(tableID)),
	); err != nil {
		t.Fatalf("create external table: %v", err)
	}

	var tableType, externalDisplay, externalJSON string
	if err := run(ctx,
		textOf("tableInfoType", &tableType),
		attrOf("tableInfoExternalSection", "style", &externalDisplay),
		textOf("tableInfoExternal", &externalJSON),
	); err != nil {
		t.Fatalf("read table overview: %v", err)
	}
	if tableType != "EXTERNAL" {
		t.Errorf("expected Type EXTERNAL, got %q", tableType)
	}
	if strings.Contains(externalDisplay, "display: none") {
		t.Errorf("expected external data configuration block to be visible, style=%q", externalDisplay)
	}
	if !strings.Contains(externalJSON, "sourceFormat") || !strings.Contains(externalJSON, "CSV") {
		t.Errorf("expected external config JSON to include sourceFormat CSV, got %q", externalJSON)
	}

	var treeLabel bool
	if err := run(ctx, evalBool(treeContainsExpr(tableID+" (external)"), &treeLabel)); err != nil {
		t.Fatalf("check explorer (external) suffix: %v", err)
	}
	if !treeLabel {
		t.Error("expected Explorer tree to mark the node with an '(external)' suffix")
	}

	// Preview must reflect live file contents through the same tabledata.list
	// endpoint a direct API call would hit.
	if err := run(ctx,
		chromedp.Click(`#tableDetailsTabs .tab[data-target="table-preview-view"]`, chromedp.ByQuery),
		pollTrue(`document.getElementById("tablePreviewTable").textContent.includes("alpha")`),
	); err != nil {
		t.Fatalf("open preview tab: %v", err)
	}

	var previewText string
	if err := run(ctx, textOf("tablePreviewTable", &previewText)); err != nil {
		t.Fatalf("read preview table: %v", err)
	}
	if !strings.Contains(previewText, "alpha") || !strings.Contains(previewText, "beta") {
		t.Errorf("expected preview to show live CSV rows alpha/beta, got %q", previewText)
	}
}
