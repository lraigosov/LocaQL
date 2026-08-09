package main

import (
	"reflect"
	"testing"
	"time"
)

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	want := config{
		addr: ":9050", storageAddr: ":9060", capabilities: "/etc/locaql/capabilities/registry.yaml", uiAddr: ":9070",
		maxRestarts: 20, restartWindow: 10 * time.Minute, restartBackoff: 2 * time.Second,
	}
	if cfg != want {
		t.Fatalf("got %+v, want %+v", cfg, want)
	}
}

func TestParseConfigOverrides(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--addr", ":19050", "--storage-grpc-addr", ":19060", "--ui-addr", ":19070", "--capabilities", "/tmp/registry.yaml",
		"--emulator-max-restarts", "5", "--emulator-restart-window", "1m", "--emulator-restart-backoff", "50ms",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	want := config{
		addr: ":19050", storageAddr: ":19060", capabilities: "/tmp/registry.yaml", uiAddr: ":19070",
		maxRestarts: 5, restartWindow: time.Minute, restartBackoff: 50 * time.Millisecond,
	}
	if cfg != want {
		t.Fatalf("got %+v, want %+v", cfg, want)
	}
}

func TestEmulatorArgs(t *testing.T) {
	cfg := config{addr: ":9050", storageAddr: ":9060", capabilities: "/etc/locaql/capabilities/registry.yaml"}
	got := cfg.emulatorArgs()
	want := []string{"start", "--addr", ":9050", "--storage-grpc-addr", ":9060", "--capabilities", "/etc/locaql/capabilities/registry.yaml"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestUIArgsPointsAtEmulatorOnLocalhost(t *testing.T) {
	cfg := config{addr: ":9050", uiAddr: ":9070"}
	got := cfg.uiArgs()
	want := []string{"--addr", ":9070", "--emulator", "http://localhost:9050"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// The restart-loop mechanics themselves (crash-loop bound, requested
// shutdown, sliding-window pruning) are tested once, against real child
// processes, in internal/procsupervisor — this package only wires that
// primitive together with the emulator/UI-specific config and args.
