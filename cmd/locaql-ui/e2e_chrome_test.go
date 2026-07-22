//go:build e2e

package main

import (
	"os"
	"os/exec"
	"testing"
)

// chromeExecPath locates a Chrome/Edge/Chromium binary for chromedp-driven
// e2e tests, on whichever OS is actually running the test binary. On a
// Windows dev machine where Go only runs inside WSL, these tests must be
// cross-compiled and executed as a native Windows binary (not under WSL) so
// the devtools websocket stays on a single loopback namespace; see the run
// instructions in cmd/locaql-ui/e2e_harness_test.go. Linux CI runners with
// google-chrome/chromium preinstalled need no such workaround.
func chromeExecPath(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"google-chrome-stable", "google-chrome", "chromium-browser", "chromium", "chrome"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	candidates := []string{
		`C:\Program Files\Google\Chrome\Application\chrome.exe`,
		`C:\Program Files (x86)\Google\Chrome\Application\chrome.exe`,
		`C:\Program Files\Microsoft\Edge\Application\msedge.exe`,
		`C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe`,
		`/Applications/Google Chrome.app/Contents/MacOS/Google Chrome`,
		`/Applications/Chromium.app/Contents/MacOS/Chromium`,
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	t.Skip("no Chrome/Edge/Chromium binary found; skipping chromedp e2e test")
	return ""
}
