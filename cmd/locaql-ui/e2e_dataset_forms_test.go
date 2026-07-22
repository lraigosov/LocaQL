//go:build e2e

package main

import "testing"

// TestE2E_DatasetFormsBasicLifecycle exercises the console.ui.resource_forms.basic
// capability end to end: validated dataset create/update/delete (with labels
// and defaultTableExpirationMs), a non-empty-dataset delete that retries with
// deleteContents=true, the Dataset Undelete form, and basic table creation +
// metadata editing.
func TestE2E_DatasetFormsBasicLifecycle(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx
	datasetID := nextID() + "_ds"

	// --- create ---
	if err := run(ctx,
		setValue("newDatasetId", datasetID),
		submitForm("createDatasetForm"),
		pollTrue(treeContainsExpr(datasetID)),
	); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// --- update: friendlyName, location, labels, defaultTableExpirationMs ---
	if err := run(ctx,
		setValue("datasetMetaDatasetId", datasetID),
		setValue("datasetFriendlyNameInput", "E2E Dataset"),
		setValue("datasetLocationInput", "US"),
		setValue("datasetExpirationInput", "3600000"),
		setValue("datasetLabelsInput", `{"team":"e2e"}`),
		submitForm("updateDatasetForm"),
		pollTrue(`document.getElementById("datasetSummaryFriendlyName").textContent === "E2E Dataset"`),
	); err != nil {
		t.Fatalf("update dataset metadata: %v", err)
	}

	var location, labels string
	if err := run(ctx,
		textOf("datasetSummaryLocation", &location),
		textOf("datasetSummaryLabels", &labels),
	); err != nil {
		t.Fatalf("read dataset summary: %v", err)
	}
	if location != "US" {
		t.Errorf("expected location US, got %q", location)
	}
	if labels != `{
  "team": "e2e"
}` {
		t.Errorf("expected labels to round-trip team=e2e, got %q", labels)
	}

	// --- basic table creation inside the dataset (also makes it non-empty) ---
	tableID := nextID() + "_tbl"
	if err := run(ctx,
		setValue("newTableDatasetId", datasetID),
		setValue("newTableId", tableID),
		submitForm("createTableForm"),
		pollTrue(treeContainsExpr(tableID)),
	); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// --- table metadata + labels editing ---
	if err := run(ctx,
		setValue("tableFriendlyNameInput", "E2E Table"),
		setValue("tableDescriptionInput", "created by e2e"),
		submitForm("updateTableMetaForm"),
		pollTrue(`document.getElementById("tableInfoDescription").textContent === "created by e2e"`),
		setValue("tableLabelsInput", `{"kind":"e2e"}`),
		clickID("updateTableLabelsBtn"),
		pollTrue(`document.getElementById("tableInfoLabels").textContent.includes("e2e")`),
	); err != nil {
		t.Fatalf("edit table metadata/labels: %v", err)
	}

	var tableName string
	if err := run(ctx, textOf("tableInfoName", &tableName)); err != nil {
		t.Fatalf("read table name: %v", err)
	}
	if tableName == "-" || tableName == "" {
		t.Errorf("expected table overview to show a resolved table name, got %q", tableName)
	}

	// --- delete a non-empty dataset: first confirm rejects without
	// deleteContents, the UI catches that and retries with
	// deleteContents=true after a second confirm ---
	if err := run(ctx,
		setValue("datasetMetaDatasetId", datasetID),
		clickID("deleteDatasetBtn"),
		pollTrue(`!`+treeContainsExpr(datasetID)),
	); err != nil {
		t.Fatalf("delete non-empty dataset with cascade retry: %v", err)
	}

	// --- undelete: dataset metadata (not table contents) should come back ---
	if err := run(ctx,
		setValue("undeleteDatasetId", datasetID),
		submitForm("undeleteDatasetForm"),
		pollTrue(treeContainsExpr(datasetID)),
	); err != nil {
		t.Fatalf("undelete dataset: %v", err)
	}

	var restoredFriendlyName string
	if err := run(ctx, textOf("datasetSummaryFriendlyName", &restoredFriendlyName)); err != nil {
		t.Fatalf("read restored dataset summary: %v", err)
	}
	if restoredFriendlyName != "E2E Dataset" {
		t.Errorf("expected undelete to restore friendlyName 'E2E Dataset', got %q", restoredFriendlyName)
	}

	var tableStillGone bool
	if err := run(ctx, evalBool(`!`+treeContainsExpr(tableID), &tableStillGone)); err != nil {
		t.Fatalf("check table absence after undelete: %v", err)
	}
	if !tableStillGone {
		t.Errorf("expected undelete to NOT restore table contents, but %q reappeared", tableID)
	}
}
