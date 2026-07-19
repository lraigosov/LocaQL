package conformance

import (
	"encoding/json"
	"fmt"
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

type Case struct {
	ID             string `yaml:"id" json:"id"`
	Method         string `yaml:"method" json:"method"`
	Path           string `yaml:"path" json:"path"`
	ExpectedStatus int    `yaml:"expected_status" json:"expected_status"`
}

type CaseResult struct {
	ID             string `json:"id"`
	Method         string `json:"method"`
	Path           string `json:"path"`
	ExpectedStatus int    `json:"expected_status"`
	ActualStatus   int    `json:"actual_status"`
	Passed         bool   `json:"passed"`
	Error          string `json:"error,omitempty"`
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
		res := CaseResult{
			ID:             tc.ID,
			Method:         tc.Method,
			Path:           tc.Path,
			ExpectedStatus: tc.ExpectedStatus,
		}

		req, err := http.NewRequest(tc.Method, report.BaseURL+tc.Path, nil)
		if err != nil {
			res.Error = err.Error()
			report.Failed++
			report.Results = append(report.Results, res)
			continue
		}

		rsp, err := client.Do(req)
		if err != nil {
			res.Error = err.Error()
			report.Failed++
			report.Results = append(report.Results, res)
			continue
		}
		res.ActualStatus = rsp.StatusCode
		_ = rsp.Body.Close()

		if rsp.StatusCode == tc.ExpectedStatus {
			res.Passed = true
			report.Passed++
		} else {
			report.Failed++
		}

		report.Results = append(report.Results, res)
	}

	return report, nil
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
	b.WriteString("| Case | Method | Path | Expected | Actual | Result |\n")
	b.WriteString("|---|---|---|---:|---:|---|\n")
	for _, c := range r.Results {
		result := "PASS"
		if !c.Passed {
			result = "FAIL"
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s | %d | %d | %s |\n", c.ID, c.Method, c.Path, c.ExpectedStatus, c.ActualStatus, result))
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
