package server

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func syncQueryWithParams(t *testing.T, s *Server, query, parameterMode string, queryParameters []map[string]any) (int, map[string]any) {
	t.Helper()
	payload := map[string]any{"query": query}
	if parameterMode != "" {
		payload["parameterMode"] = parameterMode
	}
	if queryParameters != nil {
		payload["queryParameters"] = queryParameters
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal query body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	return res.Code, out
}

func namedParam(name, typ string, value any) map[string]any {
	pv := map[string]any{}
	if value != nil {
		pv["value"] = value
	}
	return map[string]any{
		"name":           name,
		"parameterType":  map[string]any{"type": typ},
		"parameterValue": pv,
	}
}

func TestNamedQueryParametersBindRealValues(t *testing.T) {
	s := newTestServer()
	code, out := syncQueryWithParams(t, s, "SELECT @x + @y AS total", "NAMED", []map[string]any{
		namedParam("x", "INT64", "5"),
		namedParam("y", "INT64", "7"),
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if got := cellsOf(t, out, 0); got[0] != "12" {
		t.Fatalf("expected 12, got %v", got)
	}
}

func TestPositionalQueryParametersBindRealValues(t *testing.T) {
	s := newTestServer()
	code, out := syncQueryWithParams(t, s, "SELECT ? + ? AS total", "POSITIONAL", []map[string]any{
		namedParam("", "INT64", "3"),
		namedParam("", "INT64", "4"),
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if got := cellsOf(t, out, 0); got[0] != "7" {
		t.Fatalf("expected 7, got %v", got)
	}
}

func TestNamedQueryParameterModeInferredFromParameterNames(t *testing.T) {
	s := newTestServer()
	// No explicit parameterMode: a named parameter should still bind
	// correctly, inferred from the presence of a "name" field.
	code, out := syncQueryWithParams(t, s, "SELECT @greeting AS x", "", []map[string]any{
		namedParam("greeting", "STRING", "hello"),
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if got := cellsOf(t, out, 0); got[0] != "hello" {
		t.Fatalf("expected hello, got %v", got)
	}
}

func TestTypedNullQueryParameterBindsRealNull(t *testing.T) {
	s := newTestServer()
	code, out := syncQueryWithParams(t, s, "SELECT @x IS NULL AS is_null", "NAMED", []map[string]any{
		namedParam("x", "STRING", nil),
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if got := cellsOf(t, out, 0); got[0] != "true" {
		t.Fatalf("expected true, got %v", got)
	}
}

func TestQueryParameterScalarTypeCoverage(t *testing.T) {
	s := newTestServer()
	cases := []struct {
		name     string
		typ      string
		value    any
		query    string
		expected string
	}{
		{"float", "FLOAT64", "1.5", "SELECT @p + 0.5 AS x", "2"},
		{"bool", "BOOL", "true", "SELECT @p AND TRUE AS x", "true"},
		{"date", "DATE", "2026-06-15", "SELECT @p AS x", "2026-06-15"},
		{"timestamp", "TIMESTAMP", "2026-06-15 00:00:00 UTC", "SELECT @p AS x", "2026-06-15 00:00:00 UTC"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out := syncQueryWithParams(t, s, tc.query, "NAMED", []map[string]any{namedParam("p", tc.typ, tc.value)})
			if code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %v", code, out)
			}
			got := cellsOf(t, out, 0)
			if got[0] != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, got[0])
			}
		})
	}
}

func TestBytesQueryParameterRoundTripsBase64(t *testing.T) {
	s := newTestServer()
	encoded := base64.StdEncoding.EncodeToString([]byte("hello"))
	code, out := syncQueryWithParams(t, s, "SELECT @p AS x", "NAMED", []map[string]any{
		namedParam("p", "BYTES", encoded),
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if got := cellsOf(t, out, 0); got[0] != encoded {
		t.Fatalf("expected the same base64 value %v back, got %v", encoded, got)
	}
}

func TestQueryParameterRejectsUnsupportedArrayType(t *testing.T) {
	s := newTestServer()
	code, out := syncQueryWithParams(t, s, "SELECT @p AS x", "NAMED", []map[string]any{
		{
			"name":           "p",
			"parameterType":  map[string]any{"type": "ARRAY", "arrayType": map[string]any{"type": "INT64"}},
			"parameterValue": map[string]any{"arrayValues": []any{map[string]any{"value": "1"}}},
		},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unsupported ARRAY parameter type, got %d: %v", code, out)
	}
}

func TestQueryParameterRejectsNumericType(t *testing.T) {
	s := newTestServer()
	code, out := syncQueryWithParams(t, s, "SELECT @p AS x", "NAMED", []map[string]any{
		namedParam("p", "NUMERIC", "0.1"),
	})
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unsupported NUMERIC parameter type, got %d: %v", code, out)
	}
}

func TestQueryParametersWorkThroughAsyncJobsInsert(t *testing.T) {
	s := newTestServer()
	body, err := json.Marshal(map[string]any{
		"configuration": map[string]any{
			"query": map[string]any{
				"query":         "SELECT @x * 2 AS doubled",
				"parameterMode": "NAMED",
				"queryParameters": []map[string]any{
					namedParam("x", "INT64", "21"),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal jobs.insert body: %v", err)
	}
	jobOut := runJobAndFetch(t, s, string(body))
	status, _ := jobOut["status"].(map[string]any)
	if status != nil && status["errorResult"] != nil {
		t.Fatalf("unexpected job error: %v", status["errorResult"])
	}
	jobRef, ok := jobOut["jobReference"].(map[string]any)
	if !ok {
		t.Fatalf("expected jobReference in response: %v", jobOut)
	}
	jobID, _ := jobRef["jobId"].(string)
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/queries/"+url.PathEscape(jobID), nil)
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode getQueryResults response: %v", err)
	}
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", res.Code, out)
	}
	if got := cellsOf(t, out, 0); got[0] != "42" {
		t.Fatalf("expected 42, got %v", got)
	}
}
