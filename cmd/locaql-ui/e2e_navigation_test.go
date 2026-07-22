//go:build e2e

package main

import (
	"strings"
	"testing"

	"github.com/chromedp/chromedp"
)

func railActiveExpr(nav string) string {
	return `document.querySelector('.rail-icon[data-nav="` + nav + `"]').classList.contains("active")`
}

func sectionVisibleExpr(id string) string {
	return `getComputedStyle(document.getElementById("` + id + `")).display !== "none"`
}

// TestE2E_NavigationMenusFunctional exercises console.ui.navigation.menus_functional:
// appbar and side navigation actions are wired to emulator-backed views for
// search, jobs, history and capabilities.
func TestE2E_NavigationMenusFunctional(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx

	// Rail: Jobs
	if err := run(ctx,
		chromedp.Click(`.rail-icon[data-nav="jobs"]`, chromedp.ByQuery),
		pollTrue(sectionVisibleExpr("jobs-explorer")),
		pollTrue(railActiveExpr("jobs")),
	); err != nil {
		t.Fatalf("rail jobs nav: %v", err)
	}

	// Rail: History -> scopes jobs to all users (project history)
	if err := run(ctx,
		chromedp.Click(`.rail-icon[data-nav="history"]`, chromedp.ByQuery),
		pollTrue(sectionVisibleExpr("jobs-explorer")),
		pollTrue(railActiveExpr("history")),
	); err != nil {
		t.Fatalf("rail history nav: %v", err)
	}
	var allUsersChecked bool
	if err := run(ctx, evalBool(`document.getElementById("allUsersToggle").checked`, &allUsersChecked)); err != nil {
		t.Fatalf("read allUsersToggle: %v", err)
	}
	if !allUsersChecked {
		t.Error("expected rail History nav to check the allUsers/project-history toggle")
	}

	// Rail: Search -> focuses the global search input
	if err := run(ctx,
		chromedp.Click(`.rail-icon[data-nav="search"]`, chromedp.ByQuery),
		pollTrue(sectionVisibleExpr("query-workspace")),
		pollTrue(railActiveExpr("search")),
	); err != nil {
		t.Fatalf("rail search nav: %v", err)
	}
	var focusedID string
	if err := run(ctx, evalString(`document.activeElement && document.activeElement.id`, &focusedID)); err != nil {
		t.Fatalf("read focused element: %v", err)
	}
	if focusedID != "globalSearchInput" {
		t.Errorf("expected rail Search nav to focus globalSearchInput, got focus on %q", focusedID)
	}

	// Rail: Studio -> back to the query workspace
	if err := run(ctx,
		chromedp.Click(`.rail-icon[data-nav="studio"]`, chromedp.ByQuery),
		pollTrue(railActiveExpr("studio")),
		pollTrue(sectionVisibleExpr("query-workspace")),
	); err != nil {
		t.Fatalf("rail studio nav: %v", err)
	}

	// Appbar "more" button -> opens Capabilities, real registry JSON backed
	if err := run(ctx,
		clickID("appbarMoreBtn"),
		pollTrue(sectionVisibleExpr("capabilities-view")),
	); err != nil {
		t.Fatalf("appbar capabilities nav: %v", err)
	}
	var capsJSON string
	if err := run(ctx, textOf("capabilitiesJson", &capsJSON)); err != nil {
		t.Fatalf("read capabilities json: %v", err)
	}
	if !strings.Contains(capsJSON, "rest.datasets.get") {
		t.Errorf("expected capabilities view to show the real registry JSON, got %q (truncated)", firstN(capsJSON, 120))
	}

	// Appbar search button -> focuses global search and highlights rail
	if err := run(ctx,
		clickID("appbarSearchBtn"),
		pollTrue(railActiveExpr("search")),
	); err != nil {
		t.Fatalf("appbar search nav: %v", err)
	}

	// Appbar theme toggle -> flips data-theme
	var themeBefore string
	if err := run(ctx, evalString(`document.body.getAttribute("data-theme")`, &themeBefore)); err != nil {
		t.Fatalf("read theme before: %v", err)
	}
	if err := run(ctx,
		clickID("appbarThemeBtn"),
		pollTrue(`document.body.getAttribute("data-theme") !== `+jsString(themeBefore)),
	); err != nil {
		t.Fatalf("appbar theme toggle: %v", err)
	}

	// Nav collapse toggle
	if err := run(ctx,
		clickID("navCollapseBtn"),
		pollTrue(`document.body.classList.contains("nav-collapsed")`),
	); err != nil {
		t.Fatalf("nav collapse toggle: %v", err)
	}
}

func firstN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
