//go:build e2e

package main

import (
	"strings"
	"testing"
)

// TestE2E_RoutinesModelsCrud exercises console.ui.explorer.routines_models_crud:
// the Explorer renders real routine and model nodes per dataset, sidebar
// forms create either, selecting one opens a details panel (raw JSON,
// friendlyName/description edit, delete).
func TestE2E_RoutinesModelsCrud(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx
	datasetID := nextID() + "_rmds"
	routineID := nextID() + "_routine"
	modelID := nextID() + "_model"

	if err := run(ctx,
		setValue("newDatasetId", datasetID),
		submitForm("createDatasetForm"),
		pollTrue(treeContainsExpr(datasetID)),
	); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// --- Routine: create (with arguments), view details, edit description, delete ---
	if err := run(ctx,
		setValue("newRoutineDatasetId", datasetID),
		setValue("newRoutineId", routineID),
		setValue("newRoutineDefinitionBody", "x + 1"),
		setValue("newRoutineArguments", `[{"name":"x","dataType":"INT64"}]`),
		submitForm("createRoutineForm"),
		pollTrue(treeContainsExpr(routineID)),
		pollTrue(`document.getElementById("resourceDetailsTitle").textContent === "Routine Details"`),
	); err != nil {
		t.Fatalf("create routine: %v", err)
	}

	var routineJSON string
	if err := run(ctx, textOf("resourceDetailsJson", &routineJSON)); err != nil {
		t.Fatalf("read routine details json: %v", err)
	}
	if !strings.Contains(routineJSON, routineID) || !strings.Contains(routineJSON, "x + 1") {
		t.Errorf("expected routine details JSON to reflect %s / definitionBody, got %q", routineID, routineJSON)
	}
	if !strings.Contains(routineJSON, `"dataType": "INT64"`) {
		t.Errorf("expected routine details JSON to reflect the arguments submitted via the UI form, got %q", routineJSON)
	}

	if err := run(ctx,
		setValue("resourceDescriptionInput", "e2e routine desc"),
		submitForm("updateResourceMetaForm"),
		pollTrue(`document.getElementById("resourceActionStatus").textContent.includes("metadata saved")`),
	); err != nil {
		t.Fatalf("edit routine description: %v", err)
	}
	var routineJSONAfter string
	if err := run(ctx, textOf("resourceDetailsJson", &routineJSONAfter)); err != nil {
		t.Fatalf("read routine details json after edit: %v", err)
	}
	if !strings.Contains(routineJSONAfter, "e2e routine desc") {
		t.Errorf("expected updated description to round-trip into details JSON, got %q", routineJSONAfter)
	}

	if err := run(ctx,
		clickID("deleteResourceBtn"),
		pollTrue(`!`+treeContainsExpr(routineID)),
	); err != nil {
		t.Fatalf("delete routine: %v", err)
	}

	// --- Model: create, view details, edit friendlyName+description, delete ---
	if err := run(ctx,
		setValue("newModelDatasetId", datasetID),
		setValue("newModelId", modelID),
		setValue("newModelType", "LOGISTIC_REGRESSION"),
		submitForm("createModelForm"),
		pollTrue(treeContainsExpr(modelID)),
		pollTrue(`document.getElementById("resourceDetailsTitle").textContent === "Model Details"`),
	); err != nil {
		t.Fatalf("create model: %v", err)
	}

	var modelJSON string
	if err := run(ctx, textOf("resourceDetailsJson", &modelJSON)); err != nil {
		t.Fatalf("read model details json: %v", err)
	}
	if !strings.Contains(modelJSON, modelID) || !strings.Contains(modelJSON, "LOGISTIC_REGRESSION") {
		t.Errorf("expected model details JSON to reflect %s / modelType, got %q", modelID, modelJSON)
	}

	if err := run(ctx,
		setValue("resourceFriendlyNameInput", "E2E Model"),
		setValue("resourceDescriptionInput", "e2e model desc"),
		submitForm("updateResourceMetaForm"),
		pollTrue(`document.getElementById("resourceActionStatus").textContent.includes("metadata saved")`),
	); err != nil {
		t.Fatalf("edit model metadata: %v", err)
	}
	var modelJSONAfter string
	if err := run(ctx, textOf("resourceDetailsJson", &modelJSONAfter)); err != nil {
		t.Fatalf("read model details json after edit: %v", err)
	}
	if !strings.Contains(modelJSONAfter, "E2E Model") || !strings.Contains(modelJSONAfter, "e2e model desc") {
		t.Errorf("expected updated friendlyName/description to round-trip, got %q", modelJSONAfter)
	}

	if err := run(ctx,
		clickID("deleteResourceBtn"),
		pollTrue(`!`+treeContainsExpr(modelID)),
	); err != nil {
		t.Fatalf("delete model: %v", err)
	}
}
