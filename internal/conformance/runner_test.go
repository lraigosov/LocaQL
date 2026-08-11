package conformance

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// newRecordingServer replays a canned response for each registered
// "METHOD /path" key, decoding the request body as JSON before handing it to
// the matching handler.
func newRecordingServer(handlers map[string]func(w http.ResponseWriter, body map[string]any)) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&body)
		}

		h, ok := handlers[r.Method+" "+r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		h(w, body)
	}))
}

func writeCasesFile(t *testing.T, yamlContent string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cases.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("write cases file: %v", err)
	}
	return path
}

func jsonOK(w http.ResponseWriter, payload map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func TestRunPassesRequestBodyAndAssertsResponseBody(t *testing.T) {
	srv := newRecordingServer(map[string]func(http.ResponseWriter, map[string]any){
		"POST /widgets": func(w http.ResponseWriter, body map[string]any) {
			jsonOK(w, map[string]any{"echoedName": body["name"]})
		},
	})
	defer srv.Close()

	casesPath := writeCasesFile(t, `
cases:
  - id: create_widget
    method: POST
    path: /widgets
    body:
      name: gizmo
    expected_status: 200
    expected_body_contains:
      - '"echoedName":"gizmo"'
`)

	report, err := (Runner{BaseURL: srv.URL}).Run(casesPath)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Passed != 1 || report.Failed != 0 {
		t.Fatalf("got passed=%d failed=%d, want passed=1 failed=0 (results: %+v)", report.Passed, report.Failed, report.Results)
	}
}

func TestRunFailsWhenExpectedBodySubstringMissing(t *testing.T) {
	srv := newRecordingServer(map[string]func(http.ResponseWriter, map[string]any){
		"GET /widgets/1": func(w http.ResponseWriter, _ map[string]any) {
			jsonOK(w, map[string]any{"name": "gizmo"})
		},
	})
	defer srv.Close()

	casesPath := writeCasesFile(t, `
cases:
  - id: get_widget
    method: GET
    path: /widgets/1
    expected_status: 200
    expected_body_contains:
      - this-substring-is-not-present
`)

	report, err := (Runner{BaseURL: srv.URL}).Run(casesPath)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Passed != 0 || report.Failed != 1 {
		t.Fatalf("got passed=%d failed=%d, want passed=0 failed=1", report.Passed, report.Failed)
	}
	if report.Results[0].Error == "" {
		t.Fatal("expected a non-empty error describing the missing substring")
	}
}

func TestRunSkipsMainRequestWhenSetupFails(t *testing.T) {
	var mainRequestSeen bool
	srv := newRecordingServer(map[string]func(http.ResponseWriter, map[string]any){
		"POST /setup-fixture": func(w http.ResponseWriter, _ map[string]any) {
			w.WriteHeader(http.StatusBadRequest)
		},
		"GET /main": func(w http.ResponseWriter, _ map[string]any) {
			mainRequestSeen = true
			w.WriteHeader(http.StatusOK)
		},
	})
	defer srv.Close()

	casesPath := writeCasesFile(t, `
cases:
  - id: needs_fixture
    setup:
      - method: POST
        path: /setup-fixture
    method: GET
    path: /main
    expected_status: 200
`)

	report, err := (Runner{BaseURL: srv.URL}).Run(casesPath)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Passed != 0 || report.Failed != 1 {
		t.Fatalf("got passed=%d failed=%d, want passed=0 failed=1", report.Passed, report.Failed)
	}
	if report.Results[0].Error == "" {
		t.Fatal("expected a setup-failure error message")
	}
	if mainRequestSeen {
		t.Fatal("main request must not run once setup fails")
	}
}

func TestRunAlwaysRunsCleanupAndRecordsItsFailureSeparately(t *testing.T) {
	srv := newRecordingServer(map[string]func(http.ResponseWriter, map[string]any){
		"GET /main": func(w http.ResponseWriter, _ map[string]any) {
			w.WriteHeader(http.StatusOK)
		},
		"DELETE /fixture": func(w http.ResponseWriter, _ map[string]any) {
			w.WriteHeader(http.StatusBadRequest)
		},
	})
	defer srv.Close()

	casesPath := writeCasesFile(t, `
cases:
  - id: cleans_up_even_on_pass
    method: GET
    path: /main
    expected_status: 200
    cleanup:
      - method: DELETE
        path: /fixture
`)

	report, err := (Runner{BaseURL: srv.URL}).Run(casesPath)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Passed != 1 || report.Failed != 0 {
		t.Fatalf("a failing cleanup step must not flip an already-passed case: got passed=%d failed=%d", report.Passed, report.Failed)
	}
	if report.Results[0].CleanupError == "" {
		t.Fatal("expected the cleanup failure to be recorded separately from the case result")
	}
}
