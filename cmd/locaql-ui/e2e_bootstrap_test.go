//go:build e2e

package main

import "testing"

func TestE2E_Bootstrap(t *testing.T) {
	env := newE2EEnv(t)
	var health string
	if err := run(env.ctx, textOf("healthStatus", &health)); err != nil {
		t.Fatalf("read health status: %v", err)
	}
	if health != "ok" {
		t.Fatalf("expected health status 'ok', got %q", health)
	}
}
