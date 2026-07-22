//go:build e2e

package main

import (
	"testing"

	"github.com/chromedp/chromedp"
)

// TestE2E_JobsHistoryTabs exercises console.ui.jobs.history_tabs: the jobs
// explorer supports explicit personal and project history tabs mapped to
// allUsers behavior.
func TestE2E_JobsHistoryTabs(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx

	if err := run(ctx,
		setValue("queryText", "SELECT 1 AS sample"),
		submitForm("runQueryForm"),
		pollTrue(`document.getElementById("queryRunStatus").textContent.startsWith("submitted")`),
		switchMainTab("jobs-explorer"),
	); err != nil {
		t.Fatalf("submit query job + open jobs tab: %v", err)
	}

	var hint string
	var personalActive bool
	if err := run(ctx,
		textOf("jobsHistoryHint", &hint),
		evalBool(`document.querySelector('#jobsHistoryTabs .tab[data-target="personal-history"]').classList.contains("active")`, &personalActive),
	); err != nil {
		t.Fatalf("read default jobs history state: %v", err)
	}
	if hint != "Scope: personal jobs in current project" {
		t.Errorf("expected default hint for personal history, got %q", hint)
	}
	if !personalActive {
		t.Error("expected Personal History tab to be active by default")
	}

	if err := run(ctx,
		chromedp.Click(`#jobsHistoryTabs .tab[data-target="project-history"]`, chromedp.ByQuery),
		pollTrue(`document.getElementById("allUsersToggle").checked === true`),
	); err != nil {
		t.Fatalf("switch to project history: %v", err)
	}

	var hintAfter string
	var projectActive bool
	if err := run(ctx,
		textOf("jobsHistoryHint", &hintAfter),
		evalBool(`document.querySelector('#jobsHistoryTabs .tab[data-target="project-history"]').classList.contains("active")`, &projectActive),
	); err != nil {
		t.Fatalf("read project history state: %v", err)
	}
	if hintAfter != "Scope: all users in current project" {
		t.Errorf("expected project-history hint, got %q", hintAfter)
	}
	if !projectActive {
		t.Error("expected Project History tab to become active")
	}

	if err := run(ctx,
		chromedp.Click(`#jobsHistoryTabs .tab[data-target="personal-history"]`, chromedp.ByQuery),
		pollTrue(`document.getElementById("allUsersToggle").checked === false`),
	); err != nil {
		t.Fatalf("switch back to personal history: %v", err)
	}
}
