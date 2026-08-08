package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lraigosov/LocaQL/internal/procsupervisor"
)

// TestMain lets this same test binary double as the re-exec'd child in
// TestSelfRestartRecoversFromRealCrashes: when the helper env var is set,
// it runs selfRestartHelperProcess and exits instead of running the test
// suite. This is the standard Go pattern for testing subprocess-invoking
// code against the real binary rather than only against generic stand-ins
// like "false"/"sleep" (which internal/procsupervisor's own tests already
// use to test the restart-loop mechanics in isolation).
func TestMain(m *testing.M) {
	if os.Getenv("LOCAQL_TEST_SELFRESTART_HELPER") == "1" {
		os.Exit(selfRestartHelperProcess())
	}
	os.Exit(m.Run())
}

// selfRestartHelperProcess stands in for runServer: it fails (exit 1) a
// configured number of times, tracked across separate process invocations
// via a counter file, then succeeds (exit 0). It also asserts the
// invocation looks exactly like what runSelfSupervised is supposed to
// produce (a "start" subcommand and the child marker env var), so a bug in
// selfRestartChildArgs or the marker wiring fails the test loudly instead
// of silently passing.
func selfRestartHelperProcess() int {
	if len(os.Args) < 2 || os.Args[1] != "start" {
		fmt.Fprintln(os.Stderr, "self-restart helper: expected a leading 'start' subcommand, got", os.Args[1:])
		return 1
	}
	if !isSelfRestartChild() {
		fmt.Fprintln(os.Stderr, "self-restart helper: missing LOCAQL_SELF_RESTART_CHILD marker")
		return 1
	}

	counterFile := os.Getenv("LOCAQL_TEST_SELFRESTART_CRASH_FILE")
	wantCrashes, _ := strconv.Atoi(os.Getenv("LOCAQL_TEST_SELFRESTART_CRASHES"))

	count := 0
	if b, err := os.ReadFile(counterFile); err == nil {
		count, _ = strconv.Atoi(strings.TrimSpace(string(b)))
	}
	count++
	if err := os.WriteFile(counterFile, []byte(strconv.Itoa(count)), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "self-restart helper: write counter:", err)
		return 1
	}

	if count <= wantCrashes {
		return 1 // simulate the known WASM-bridge crash
	}
	return 0 // recovered; Supervise should stop restarting after this
}

func TestParseStartFlagsDefaults(t *testing.T) {
	cfg, err := parseStartFlags(nil)
	if err != nil {
		t.Fatalf("parseStartFlags: %v", err)
	}
	want := startConfig{
		addr: ":9050", storageGRPCAddr: ":9060", capPath: "capabilities/registry.yaml", selfRestart: false,
		restartPolicy: procsupervisor.RestartPolicy{MaxRestarts: 20, Window: 10 * time.Minute, Backoff: 2 * time.Second},
	}
	if cfg != want {
		t.Fatalf("got %+v, want %+v", cfg, want)
	}
}

func TestParseStartFlagsSelfRestartOverrides(t *testing.T) {
	cfg, err := parseStartFlags([]string{
		"--self-restart", "--self-restart-max-restarts", "5", "--self-restart-window", "1m", "--self-restart-backoff", "50ms",
	})
	if err != nil {
		t.Fatalf("parseStartFlags: %v", err)
	}
	if !cfg.selfRestart {
		t.Fatalf("expected selfRestart to be true")
	}
	want := procsupervisor.RestartPolicy{MaxRestarts: 5, Window: time.Minute, Backoff: 50 * time.Millisecond}
	if cfg.restartPolicy != want {
		t.Fatalf("got policy %+v, want %+v", cfg.restartPolicy, want)
	}
}

func TestSelfRestartChildArgsPrependsStartSubcommand(t *testing.T) {
	got := selfRestartChildArgs([]string{"--self-restart", "--addr", ":9999"})
	want := []string{"start", "--self-restart", "--addr", ":9999"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestIsSelfRestartChildRespectsEnvVar(t *testing.T) {
	if isSelfRestartChild() {
		t.Fatalf("expected isSelfRestartChild to be false without the marker env var set")
	}

	t.Setenv("LOCAQL_SELF_RESTART_CHILD", "1")
	if !isSelfRestartChild() {
		t.Fatalf("expected isSelfRestartChild to be true once the marker env var is set")
	}
}

// TestSelfRestartRecoversFromRealCrashes proves the actual --self-restart
// wiring works end-to-end, through this real binary's own re-exec path
// (not just the generic restart-loop mechanics already covered in
// internal/procsupervisor): a child that crashes twice must be restarted
// automatically and transparently recover on its third run.
func TestSelfRestartRecoversFromRealCrashes(t *testing.T) {
	counterFile := filepath.Join(t.TempDir(), "crash-counter")
	t.Setenv("LOCAQL_TEST_SELFRESTART_HELPER", "1")
	t.Setenv("LOCAQL_TEST_SELFRESTART_CRASH_FILE", counterFile)
	t.Setenv("LOCAQL_TEST_SELFRESTART_CRASHES", "2")

	policy := procsupervisor.RestartPolicy{MaxRestarts: 5, Window: time.Minute, Backoff: 10 * time.Millisecond}

	done := make(chan error, 1)
	go func() { done <- runSelfSupervised([]string{"--self-restart"}, policy) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected runSelfSupervised to recover and return nil, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("runSelfSupervised did not return promptly after the helper recovered")
	}

	b, err := os.ReadFile(counterFile)
	if err != nil {
		t.Fatalf("reading crash counter: %v", err)
	}
	if got := strings.TrimSpace(string(b)); got != "3" {
		t.Fatalf("expected the helper to have run 3 times (2 crashes + 1 recovery), counter file says %q", got)
	}
}
