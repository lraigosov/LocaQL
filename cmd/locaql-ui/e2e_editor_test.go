//go:build e2e

package main

import (
	"strings"
	"testing"
)

// TestE2E_KeyboardShortcuts exercises console.ui.shortcuts: Ctrl/Cmd+Enter
// runs the query job and Ctrl/Cmd+S saves it, both bound locally in the SQL
// editor's keydown handler.
func TestE2E_KeyboardShortcuts(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx

	if err := run(ctx,
		setValue("queryText", "SELECT 1 AS sample"),
		dispatchKeydown("queryText", "Enter", true),
		pollTrue(`document.getElementById("queryRunStatus").textContent.startsWith("submitted")`),
	); err != nil {
		t.Fatalf("Ctrl+Enter run shortcut: %v", err)
	}

	queryName := nextID() + "_query"
	if err := run(ctx,
		setValue("savedQueryName", queryName),
		dispatchKeydown("queryText", "s", true),
		pollTrue(`document.getElementById("queryRunStatus").textContent === "query saved"`),
	); err != nil {
		t.Fatalf("Ctrl+S save shortcut: %v", err)
	}

	var savedList string
	if err := run(ctx, textOf("savedQueriesList", &savedList)); err != nil {
		t.Fatalf("read saved queries list: %v", err)
	}
	if !strings.Contains(savedList, queryName) {
		t.Errorf("expected saved queries list to contain %q, got %q", queryName, savedList)
	}
}

// TestE2E_ExplorerSearch exercises console.ui.explorer.search: local search
// filters the Explorer tree by project/dataset/table name over the cached
// resources, and Clear resets the filter.
func TestE2E_ExplorerSearch(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx

	matchID := nextID() + "_alphamatch"
	otherID := nextID() + "_zzzother"

	if err := run(ctx,
		setValue("newDatasetId", matchID),
		submitForm("createDatasetForm"),
		pollTrue(treeContainsExpr(matchID)),
		setValue("newDatasetId", otherID),
		submitForm("createDatasetForm"),
		pollTrue(treeContainsExpr(otherID)),
	); err != nil {
		t.Fatalf("create two datasets: %v", err)
	}

	if err := run(ctx,
		setValue("explorerSearchInput", "alphamatch"),
		pollTrue(`!`+treeContainsExpr(otherID)),
	); err != nil {
		t.Fatalf("search filter did not hide non-matching dataset: %v", err)
	}

	var stillHasMatch bool
	if err := run(ctx, evalBool(treeContainsExpr(matchID), &stillHasMatch)); err != nil {
		t.Fatalf("check matching dataset still visible: %v", err)
	}
	if !stillHasMatch {
		t.Errorf("expected matching dataset %q to remain visible while filtered", matchID)
	}

	if err := run(ctx,
		clickID("clearExplorerSearchBtn"),
		pollTrue(treeContainsExpr(otherID)),
	); err != nil {
		t.Fatalf("clear search did not restore full tree: %v", err)
	}
}
