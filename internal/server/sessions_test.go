package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// runSyncQuery POSTs body to /bigquery/v2/projects/p1/queries as the given
// user and returns the decoded response, failing the test if the HTTP
// status isn't 200.
func runSyncQuery(t *testing.T, s *Server, userEmail, body string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries?userEmail="+userEmail, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v (status %d)", err, res.Code)
	}
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", res.Code, out)
	}
	return out
}

// runSyncQueryExpectStatus is runSyncQuery but for callers that expect a
// non-200 response.
func runSyncQueryExpectStatus(t *testing.T, s *Server, userEmail, body string, wantStatus int) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries?userEmail="+userEmail, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	if res.Code != wantStatus {
		t.Fatalf("expected %d, got %d: %v", wantStatus, res.Code, out)
	}
	return out
}

func sessionIDFrom(t *testing.T, resp map[string]any) string {
	t.Helper()
	info, ok := resp["sessionInfo"].(map[string]any)
	if !ok {
		t.Fatalf("expected sessionInfo in response, got %v", resp)
	}
	id, _ := info["sessionId"].(string)
	if id == "" {
		t.Fatalf("expected a non-empty sessionId, got %v", info)
	}
	return id
}

func firstRowFirstCell(t *testing.T, resp map[string]any) string {
	t.Helper()
	rows, ok := resp["rows"].([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("expected at least one row, got %v", resp["rows"])
	}
	cells := rows[0].(map[string]any)["f"].([]any)
	v, _ := cells[0].(map[string]any)["v"].(string)
	return v
}

func TestSyncQueryCreateSessionReturnsSessionInfo(t *testing.T) {
	s := newTestServer()
	resp := runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT 1 AS one","createSession":true}`)
	sessionIDFrom(t, resp)
}

func TestSessionTempTablePersistsAcrossSeparateSyncQueries(t *testing.T) {
	s := newTestServer()

	created := runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT 1 AS one","createSession":true}`)
	sessionID := sessionIDFrom(t, created)

	createTemp := `{"query":"CREATE TEMP TABLE mytemp AS SELECT 42 AS x","connectionProperties":[{"key":"session_id","value":"` + sessionID + `"}]}`
	runSyncQuery(t, s, "alice@example.com", createTemp)

	selectTemp := `{"query":"SELECT x FROM _SESSION.mytemp","connectionProperties":[{"key":"session_id","value":"` + sessionID + `"}]}`
	resp := runSyncQuery(t, s, "alice@example.com", selectTemp)
	if got := firstRowFirstCell(t, resp); got != "42" {
		t.Fatalf("expected the session temp table to round-trip x=42 across separate queries, got %q", got)
	}
}

func TestSessionUnknownSessionIdFailsExplicitly(t *testing.T) {
	s := newTestServer()
	body := `{"query":"SELECT 1","connectionProperties":[{"key":"session_id","value":"session_doesnotexist"}]}`
	runSyncQueryExpectStatus(t, s, "alice@example.com", body, http.StatusBadRequest)
}

func TestSessionTransactionCommitPersistsTempTableChanges(t *testing.T) {
	s := newTestServer()
	created := runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT 1","createSession":true}`)
	sessionID := sessionIDFrom(t, created)
	connProp := `"connectionProperties":[{"key":"session_id","value":"` + sessionID + `"}]`

	runSyncQuery(t, s, "alice@example.com", `{"query":"BEGIN TRANSACTION",`+connProp+`}`)
	runSyncQuery(t, s, "alice@example.com", `{"query":"CREATE TEMP TABLE committed_temp AS SELECT 7 AS x",`+connProp+`}`)
	runSyncQuery(t, s, "alice@example.com", `{"query":"COMMIT TRANSACTION",`+connProp+`}`)

	resp := runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT x FROM _SESSION.committed_temp",`+connProp+`}`)
	if got := firstRowFirstCell(t, resp); got != "7" {
		t.Fatalf("expected the committed temp table to be visible with x=7, got %q", got)
	}
}

func TestSessionTransactionRollbackDiscardsTempTableChanges(t *testing.T) {
	s := newTestServer()
	created := runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT 1","createSession":true}`)
	sessionID := sessionIDFrom(t, created)
	connProp := `"connectionProperties":[{"key":"session_id","value":"` + sessionID + `"}]`

	runSyncQuery(t, s, "alice@example.com", `{"query":"BEGIN TRANSACTION",`+connProp+`}`)
	runSyncQuery(t, s, "alice@example.com", `{"query":"CREATE TEMP TABLE rolled_back_temp AS SELECT 99 AS x",`+connProp+`}`)
	runSyncQuery(t, s, "alice@example.com", `{"query":"ROLLBACK TRANSACTION",`+connProp+`}`)

	runSyncQueryExpectStatus(t, s, "alice@example.com", `{"query":"SELECT x FROM _SESSION.rolled_back_temp",`+connProp+`}`, http.StatusBadRequest)
}

func TestSessionRollbackWithoutActiveTransactionFailsExplicitly(t *testing.T) {
	s := newTestServer()
	created := runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT 1","createSession":true}`)
	sessionID := sessionIDFrom(t, created)
	connProp := `"connectionProperties":[{"key":"session_id","value":"` + sessionID + `"}]`

	runSyncQueryExpectStatus(t, s, "alice@example.com", `{"query":"ROLLBACK TRANSACTION",`+connProp+`}`, http.StatusBadRequest)
}

func TestSessionIdleTimeoutExpiresSession(t *testing.T) {
	s := newTestServer()
	current := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.sessions.now = func() time.Time { return current }

	created := runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT 1","createSession":true}`)
	sessionID := sessionIDFrom(t, created)

	current = current.Add(s.sessions.idleTimeout + time.Minute)

	connProp := `"connectionProperties":[{"key":"session_id","value":"` + sessionID + `"}]`
	runSyncQueryExpectStatus(t, s, "alice@example.com", `{"query":"SELECT 1",`+connProp+`}`, http.StatusBadRequest)
}

func TestInformationSchemaSessionsByUserListsOnlyOwnSessions(t *testing.T) {
	s := newTestServer()
	aliceCreated := runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT 1","createSession":true}`)
	aliceSessionID := sessionIDFrom(t, aliceCreated)
	runSyncQuery(t, s, "bob@example.com", `{"query":"SELECT 1","createSession":true}`)

	resp := runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT * FROM p1.INFORMATION_SCHEMA.SESSIONS_BY_USER"}`)
	rows, ok := resp["rows"].([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("expected at least one row for alice's own session, got %v", resp["rows"])
	}
	for _, row := range rows {
		cells := row.(map[string]any)["f"].([]any)
		sessionID, _ := cells[1].(map[string]any)["v"].(string)
		if sessionID != aliceSessionID {
			t.Fatalf("expected only alice's session (%s), found %s", aliceSessionID, sessionID)
		}
	}
}

func TestInformationSchemaSessionsByUserWithoutCallerReturnsEmpty(t *testing.T) {
	s := newTestServer()
	runSyncQuery(t, s, "alice@example.com", `{"query":"SELECT 1","createSession":true}`)

	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(`{"query":"SELECT * FROM p1.INFORMATION_SCHEMA.SESSIONS_BY_USER"}`))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", res.Code, out)
	}
	if rows, ok := out["rows"].([]any); ok && len(rows) > 0 {
		t.Fatalf("expected zero rows without a calling user, got %v", rows)
	}
}

// TestAsyncSingleStatementWithTrailingSemicolonIsNotTreatedAsScript pins a
// real, independently-found bug fix: insertJob previously flagged any query
// containing a semicolon anywhere as a multi-statement "script" job
// (strings.Count(queryText, ";") > 0), which misclassified an ordinary
// single statement with a normal trailing semicolon — exactly how session
// control statements (BEGIN TRANSACTION;, COMMIT TRANSACTION;) are
// naturally written. Fixed to use splitScriptStatements (already used
// elsewhere for real script splitting) so only a genuine multi-statement
// body is treated as a script.
func TestAsyncSingleStatementWithTrailingSemicolonIsNotTreatedAsScript(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs?userEmail=alice@example.com", strings.NewReader(`{"configuration":{"query":{"query":"SELECT 1 AS one;"}}}`))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.Code)
	}
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, hasChildren := out["children"]; hasChildren {
		t.Fatalf("expected a single job response (no children), got %v", out)
	}
	if jobType, _ := out["jobType"].(string); jobType != "query" {
		t.Fatalf("expected jobType=query, not treated as a script, got %v", out["jobType"])
	}
}

func TestAsyncGenuineMultiStatementScriptStillTreatedAsScript(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs?userEmail=alice@example.com", strings.NewReader(`{"configuration":{"query":{"query":"SELECT 1; SELECT 2;"}}}`))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK && res.Code != http.StatusCreated {
		t.Fatalf("expected 200/201, got %d", res.Code)
	}
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	children, ok := out["children"].([]any)
	if !ok || len(children) != 2 {
		t.Fatalf("expected 2 children for a genuine 2-statement script, got %v", out["children"])
	}
}
