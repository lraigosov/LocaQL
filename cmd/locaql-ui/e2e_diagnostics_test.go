//go:build e2e

package main

import (
	"strings"
	"testing"

	"github.com/chromedp/chromedp"
)

// TestE2E_LegalDisclaimerVisible exercises the console's non-affiliation
// disclaimer (master plan §43.1): visible in the sidebar brand block by
// default, and present as a persistent appbar subtitle that survives the
// sidebar being collapsed.
func TestE2E_LegalDisclaimerVisible(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx

	// .appbar-brand-subtitle applies text-transform: uppercase, which
	// chromedp.Text (rendered innerText, not raw textContent) reflects — so
	// this compares case-insensitively rather than asserting a literal case.
	var sidebarNote string
	if err := run(ctx, textOf("appbarDisclaimer", &sidebarNote)); err != nil {
		t.Fatalf("read appbar disclaimer: %v", err)
	}
	if !strings.Contains(strings.ToLower(sidebarNote), "not affiliated") {
		t.Errorf("expected appbar disclaimer to state non-affiliation, got %q", sidebarNote)
	}

	var fullText string
	if err := run(ctx, evalString(`document.querySelector(".brand-note").textContent`, &fullText)); err != nil {
		t.Fatalf("read sidebar brand note: %v", err)
	}
	for _, want := range []string{"independent", "not affiliated with, sponsored by, or endorsed by Google LLC"} {
		if !strings.Contains(fullText, want) {
			t.Errorf("expected sidebar disclaimer to contain %q, got %q", want, fullText)
		}
	}

	// Collapsing the nav must not hide the disclaimer entirely — the appbar
	// subtitle is outside the collapsible sidebar.
	if err := run(ctx,
		clickID("navCollapseBtn"),
		pollTrue(`document.body.classList.contains("nav-collapsed")`),
	); err != nil {
		t.Fatalf("collapse nav: %v", err)
	}
	var visibleAfterCollapse bool
	if err := run(ctx, evalBool(`getComputedStyle(document.getElementById("appbarDisclaimer")).display !== "none"`, &visibleAfterCollapse)); err != nil {
		t.Fatalf("read disclaimer visibility: %v", err)
	}
	if !visibleAfterCollapse {
		t.Error("expected the appbar disclaimer to stay visible after collapsing the sidebar")
	}
}

// TestE2E_VersionAndDiagnosticsPanels exercises the version handshake
// (master plan §43.4) and the guided-troubleshooting panel (§43.3
// "Diagnóstico de compatibilidad"): both call real emulator endpoints
// (/_emulator/version, /_emulator/diagnostics), not fabricated data.
func TestE2E_VersionAndDiagnosticsPanels(t *testing.T) {
	env := newE2EEnv(t)
	ctx := env.ctx

	var version string
	if err := run(ctx, textOf("versionStatus", &version)); err != nil {
		t.Fatalf("read version status: %v", err)
	}
	if version == "" || version == "-" {
		t.Fatalf("expected a real version string, got %q", version)
	}

	if err := run(ctx,
		chromedp.Click(`.tab[data-target="diagnostics-view"]`, chromedp.ByQuery),
		pollTrue(sectionVisibleExpr("diagnostics-view")),
	); err != nil {
		t.Fatalf("open diagnostics tab: %v", err)
	}
	var diagnosticsJSON string
	if err := run(ctx, textOf("diagnosticsJson", &diagnosticsJSON)); err != nil {
		t.Fatalf("read diagnostics json: %v", err)
	}
	for _, want := range []string{"persistence", "resourceLocks", "sessions"} {
		if !strings.Contains(diagnosticsJSON, want) {
			t.Errorf("expected diagnostics view to show the real /_emulator/diagnostics payload (missing %q), got %q", want, firstN(diagnosticsJSON, 200))
		}
	}
}
