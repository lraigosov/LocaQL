package capabilities

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRegistry(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "registry.yaml")
	content := []byte("capabilities:\n  emulator.health:\n    status: supported\n    fidelity: high\n")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	reg, err := Load(path)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if len(reg.Capabilities) != 1 {
		t.Fatalf("expected 1 capability, got %d", len(reg.Capabilities))
	}
}
