package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lraigosov/LocaQL/internal/capabilities"
)

func TestHealthEndpoint(t *testing.T) {
	s := New(capabilities.Registry{Capabilities: map[string]capabilities.Entry{"emulator.health": {Status: "supported", Fidelity: "high"}}})
	req := httptest.NewRequest(http.MethodGet, "/_emulator/health", nil)
	res := httptest.NewRecorder()

	s.Handler().ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
	}
}

func TestReadinessEndpoint(t *testing.T) {
	s := New(capabilities.Registry{Capabilities: map[string]capabilities.Entry{"emulator.readiness": {Status: "supported", Fidelity: "high"}}})
	req := httptest.NewRequest(http.MethodGet, "/_emulator/readiness", nil)
	res := httptest.NewRecorder()

	s.Handler().ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
	}
}

func TestCapabilitiesEndpoint(t *testing.T) {
	reg := capabilities.Registry{Capabilities: map[string]capabilities.Entry{"emulator.health": {Status: "supported", Fidelity: "high"}}}
	s := New(reg)
	req := httptest.NewRequest(http.MethodGet, "/_emulator/capabilities", nil)
	res := httptest.NewRecorder()

	s.Handler().ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
	}

	var got capabilities.Registry
	if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got.Capabilities) != 1 {
		t.Fatalf("expected one capability, got %d", len(got.Capabilities))
	}
}
