//go:build e2e

// Package main e2e tests drive the real console UI against a real emulator
// backend inside a headless Chrome instance, to verify console.ui.* registry
// capabilities against actual behavior instead of code-reading alone.
//
// These tests must run as a native Windows binary, not under WSL: Chrome on
// this dev machine is a native Windows process, and the DevTools protocol
// only works within a single OS network namespace. Build and run like:
//
//	GOOS=windows GOARCH=amd64 go test -tags e2e -c -o e2e.exe ./cmd/locaql-ui
//	(copy e2e.exe to a Windows-reachable path, then, from PowerShell:)
//	./e2e.exe -test.v
package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	"github.com/lraigosov/LocaQL/internal/capabilities"
	"github.com/lraigosov/LocaQL/internal/server"
)

type e2eEnv struct {
	t   *testing.T
	ctx context.Context
	dir string
}

var e2eSeq int64

// nextID returns a short unique suffix so parallel-safe naming stays cheap
// without needing time.Now (tests may run with -test.count>1).
func nextID() string {
	return fmt.Sprintf("e2e%d", atomic.AddInt64(&e2eSeq, 1))
}

func registryPath(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "capabilities", "registry.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("locate capabilities registry: %v", err)
	}
	return path
}

// newE2EEnv boots a real emulator + UI proxy in-process, launches headless
// Chrome, navigates to the console, and grants clipboard permissions so
// share-link and similar flows behave like a real user session.
func newE2EEnv(t *testing.T) *e2eEnv {
	t.Helper()

	reg, err := capabilities.Load(registryPath(t))
	if err != nil {
		t.Fatalf("load capabilities registry: %v", err)
	}
	srv := server.New(reg)
	emulator := httptest.NewServer(srv.Handler())
	t.Cleanup(emulator.Close)

	handler, err := newHandler(emulator.URL)
	if err != nil {
		t.Fatalf("new ui handler: %v", err)
	}
	ui := httptest.NewServer(handler)
	t.Cleanup(ui.Close)

	dir := t.TempDir()

	allocOpts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.ExecPath(chromeExecPath(t)),
		chromedp.Flag("headless", true),
		chromedp.WindowSize(1600, 1200),
		chromedp.Flag("disable-gpu", true),
	)
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), allocOpts...)
	// Silence noisy-but-harmless cdproto/Chrome protocol-version mismatch
	// logs (e.g. IPAddressSpace enum drift) that would otherwise spam every
	// test's output without affecting results.
	ctx, cancelCtx := chromedp.NewContext(allocCtx, chromedp.WithErrorf(func(string, ...interface{}) {}))
	ctx, cancelTimeout := context.WithTimeout(ctx, 150*time.Second)

	t.Cleanup(func() {
		cancelTimeout()
		cancelCtx()
		cancelAlloc()
	})

	// Auto-accept confirm()/prompt() dialogs the way a cooperative human
	// tester would; individual tests can override promptText via context.
	chromedp.ListenTarget(ctx, func(ev interface{}) {
		if e, ok := ev.(*page.EventJavascriptDialogOpening); ok {
			go func() {
				_ = chromedp.Run(ctx, page.HandleJavaScriptDialog(true).WithPromptText(dialogPromptText(e)))
			}()
		}
	})

	if err := chromedp.Run(ctx,
		browser.SetPermission(&browser.PermissionDescriptor{Name: "clipboard-read"}, browser.PermissionSettingGranted).WithOrigin(ui.URL),
		browser.SetPermission(&browser.PermissionDescriptor{Name: "clipboard-write"}, browser.PermissionSettingGranted).WithOrigin(ui.URL),
		chromedp.Navigate(ui.URL),
		chromedp.WaitVisible(`#explorerTree`, chromedp.ByID),
	); err != nil {
		t.Fatalf("bootstrap console: %v", err)
	}

	return &e2eEnv{t: t, ctx: ctx, dir: dir}
}

// nextDialogPromptText holds the value the next window.prompt() dialog
// should resolve to; window.confirm() ignores it and is always accepted.
var nextDialogPromptText atomic.Value

func init() {
	nextDialogPromptText.Store("")
}

func setNextPromptText(v string) {
	nextDialogPromptText.Store(v)
}

func dialogPromptText(e *page.EventJavascriptDialogOpening) string {
	if e.Type == page.DialogTypePrompt {
		if v, _ := nextDialogPromptText.Load().(string); v != "" {
			nextDialogPromptText.Store("")
			return v
		}
		return e.DefaultPrompt
	}
	return ""
}
