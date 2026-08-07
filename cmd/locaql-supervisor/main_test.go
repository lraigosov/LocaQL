package main

import (
	"reflect"
	"testing"
)

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	want := config{addr: ":9050", storageAddr: ":9060", capabilities: "/etc/locaql/capabilities/registry.yaml", uiAddr: ":9070"}
	if cfg != want {
		t.Fatalf("got %+v, want %+v", cfg, want)
	}
}

func TestParseConfigOverrides(t *testing.T) {
	cfg, err := parseConfig([]string{"--addr", ":19050", "--storage-grpc-addr", ":19060", "--ui-addr", ":19070", "--capabilities", "/tmp/registry.yaml"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	want := config{addr: ":19050", storageAddr: ":19060", capabilities: "/tmp/registry.yaml", uiAddr: ":19070"}
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
