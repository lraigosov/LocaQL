package conformance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type caseFile struct {
	Cases []Case `yaml:"cases"`
}

// Step is a single setup or cleanup request run before/after a Case's own
// request, so a case can build (and always tear down) the fixtures it needs
// — a dataset, a table, a few streamed rows — without depending on state
// left behind by a previous run or another case.
type Step struct {
	Method string `yaml:"method" json:"method"`
	Path   string `yaml:"path" json:"path"`
	Body   any    `yaml:"body,omitempty" json:"body,omitempty"`
}

type Case struct {
	ID     string `yaml:"id" json:"id"`
	Method string `yaml:"method" json:"method"`
	Path   string `yaml:"path" json:"path"`
	Body   any    `yaml:"body,omitempty" json:"body,omitempty"`
	// Setup steps run before the case's own request, in order. If any setup
	// step fails, the case is recorded as failed and its own request is
	// skipped, but Cleanup still runs (best effort) to avoid leaking
	// partially-created fixtures.
	Setup          []Step `yaml:"setup,omitempty" json:"setup,omitempty"`
	ExpectedStatus int    `yaml:"expected_status" json:"expected_status"`
	// ExpectedBodyContains is a bounded substring check against the raw
	// response body — deliberately not a JSON-path/schema engine, since a
	// literal substring is enough to prove the response carries the field or
	// value under test.
	ExpectedBodyContains []string `yaml:"expected_body_contains,omitempty" json:"expected_body_contains,omitempty"`
	// Cleanup steps always run after the case's own request, regardless of
	// whether Setup or the case itself passed or failed.
	Cleanup []Step `yaml:"cleanup,omitempty" json:"cleanup,omitempty"`
}

type CaseResult struct {
	ID             string `json:"id"`
	Method         string `json:"method"`
	Path           string `json:"path"`
	ExpectedStatus int    `json:"expected_status"`
	ActualStatus   int    `json:"actual_status"`
	Passed         bool   `json:"passed"`
	Error          string `json:"error,omitempty"`
	CleanupError   string `json:"cleanup_error,omitempty"`
}

type Report struct {
	Suite     string       `json:"suite"`
	StartedAt string       `json:"started_at"`
	BaseURL   string       `json:"base_url"`
	Passed    int          `json:"passed"`
	Failed    int          `json:"failed"`
	Results   []CaseResult `json:"results"`
}

type Runner struct {
	BaseURL string
	Client  *http.Client
}

func (r Runner) Run(casesPath string) (Report, error) {
	cases, err := loadCases(casesPath)
	if err != nil {
		return Report{}, err
	}

	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	suite := strings.TrimSuffix(filepath.Base(casesPath), filepath.Ext(casesPath))
	if suite == "" {
		suite = "conformance"
	}

	report := Report{
		Suite:     suite,
		StartedAt: time.Now().UTC().Format(time.RFC3339),
		BaseURL:   strings.TrimRight(r.BaseURL, "/"),
		Results:   make([]CaseResult, 0, len(cases)),
	}

	for _, tc := range cases {
		report.recordCase(client, report.BaseURL, tc)
	}

	return report, nil
}

// recordCase runs a single case's setup/request/cleanup lifecycle and
// appends its result, updating the running pass/fail counters. Cleanup
// always runs, even when setup or the case's own request failed.
func (r *Report) recordCase(client *http.Client, baseURL string, tc Case) {
	res := CaseResult{
		ID:             tc.ID,
		Method:         tc.Method,
		Path:           tc.Path,
		ExpectedStatus: tc.ExpectedStatus,
	}

	if err := runSteps(client, baseURL, tc.Setup); err != nil {
		res.Error = fmt.Sprintf("setup failed: %s", err)
		if cleanupErr := runSteps(client, baseURL, tc.Cleanup); cleanupErr != nil {
			res.CleanupError = cleanupErr.Error()
		}
		r.Failed++
		r.Results = append(r.Results, res)
		return
	}

	rsp, body, err := doRequest(client, tc.Method, baseURL+tc.Path, tc.Body)
	switch {
	case err != nil:
		res.Error = err.Error()
	default:
		res.ActualStatus = rsp.StatusCode
		res.Passed = rsp.StatusCode == tc.ExpectedStatus
		if res.Passed {
			if missing := firstMissingSubstring(string(body), tc.ExpectedBodyContains); missing != "" {
				res.Passed = false
				res.Error = fmt.Sprintf("response body does not contain %q", missing)
			}
		}
	}

	if cleanupErr := runSteps(client, baseURL, tc.Cleanup); cleanupErr != nil {
		res.CleanupError = cleanupErr.Error()
	}

	if res.Passed {
		r.Passed++
	} else {
		r.Failed++
	}
	r.Results = append(r.Results, res)
}

// runSteps executes every step in order and collects every failure rather
// than stopping at the first one, so a cleanup call always gets to attempt
// every one of its steps (e.g. deleting a table before its parent dataset)
// even if an earlier step in the same list already failed.
func runSteps(client *http.Client, baseURL string, steps []Step) error {
	var failures []string
	for _, st := range steps {
		rsp, _, err := doRequest(client, st.Method, baseURL+st.Path, st.Body)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s %s: %s", st.Method, st.Path, err))
			continue
		}
		if rsp.StatusCode >= http.StatusBadRequest {
			failures = append(failures, fmt.Sprintf("%s %s: unexpected status %d", st.Method, st.Path, rsp.StatusCode))
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return errors.New(strings.Join(failures, "; "))
}

func doRequest(client *http.Client, method, url string, body any) (*http.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, nil, fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rsp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rsp.Body.Close() }()

	content, err := io.ReadAll(rsp.Body)
	if err != nil {
		return rsp, nil, err
	}
	return rsp, content, nil
}

func firstMissingSubstring(haystack string, needles []string) string {
	for _, needle := range needles {
		if !strings.Contains(haystack, needle) {
			return needle
		}
	}
	return ""
}

func (r Report) WriteJSON(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}

	content, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report json: %w", err)
	}

	if err := os.WriteFile(path, content, 0o644); err != nil {
		return fmt.Errorf("write report json: %w", err)
	}
	return nil
}

func (r Report) WriteMarkdown(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}

	var b strings.Builder
	titleSuite := strings.ToUpper(r.Suite[:1]) + r.Suite[1:]
	b.WriteString(fmt.Sprintf("# %s Conformance Report\n\n", titleSuite))
	b.WriteString(fmt.Sprintf("- Suite: %s\n", r.Suite))
	b.WriteString(fmt.Sprintf("- Started at: %s\n", r.StartedAt))
	b.WriteString(fmt.Sprintf("- Base URL: %s\n", r.BaseURL))
	b.WriteString(fmt.Sprintf("- Passed: %d\n", r.Passed))
	b.WriteString(fmt.Sprintf("- Failed: %d\n\n", r.Failed))
	b.WriteString("| Case | Method | Path | Expected | Actual | Result | Error |\n")
	b.WriteString("|---|---|---|---:|---:|---|---|\n")
	for _, c := range r.Results {
		result := "PASS"
		if !c.Passed {
			result = "FAIL"
		}
		errCell := c.Error
		if c.CleanupError != "" {
			errCell = strings.TrimSpace(errCell + "; cleanup: " + c.CleanupError)
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s | %d | %d | %s | %s |\n", c.ID, c.Method, c.Path, c.ExpectedStatus, c.ActualStatus, result, errCell))
	}

	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("write report markdown: %w", err)
	}
	return nil
}

func loadCases(path string) ([]Case, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read conformance cases: %w", err)
	}

	var file caseFile
	if err := yaml.Unmarshal(content, &file); err != nil {
		return nil, fmt.Errorf("parse conformance cases: %w", err)
	}

	return file.Cases, nil
}
