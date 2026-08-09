package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func seedPersistentSQLTable(t *testing.T, s *Server) {
	t.Helper()
	if !s.datasets.exists("p1", "analytics") {
		t.Fatal("expected default analytics dataset")
	}
	_, created := s.tables.insert(tableInsert{
		ProjectID: "p1",
		DatasetID: "analytics",
		TableID:   "items",
		Schema: []tableField{
			{Name: "id", Type: "INT64", Mode: "REQUIRED"},
			{Name: "name", Type: "STRING"},
			{Name: "active", Type: "BOOL"},
		},
		Rows: [][]string{{"1", "one", "true"}, {"2", storedNullCell, "false"}},
	})
	if !created {
		t.Fatal("seed table was not created")
	}
}

func TestPersistentDMLMutatesCatalog(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)

	tests := []struct {
		query        string
		statement    string
		affectedRows int64
		wantRows     [][]string
	}{
		{
			query:        "INSERT INTO analytics.items (id, name, active) VALUES (3, '', FALSE)",
			statement:    "INSERT",
			affectedRows: 1,
			wantRows:     [][]string{{"1", "one", "true"}, {"2", storedNullCell, "false"}, {"3", "", "false"}},
		},
		{
			query:        "UPDATE analytics.items SET name = 'updated', active = TRUE WHERE id = 2",
			statement:    "UPDATE",
			affectedRows: 1,
			wantRows:     [][]string{{"1", "one", "true"}, {"2", "updated", "true"}, {"3", "", "false"}},
		},
		{
			query:        "DELETE FROM analytics.items WHERE id = 1",
			statement:    "DELETE",
			affectedRows: 1,
			wantRows:     [][]string{{"2", "updated", "true"}, {"3", "", "false"}},
		},
	}

	for _, tc := range tests {
		result, err := s.executeQueryStatement("p1", "", tc.query, "", "", nil)
		if err != nil {
			t.Fatalf("%s failed: %v", tc.statement, err)
		}
		if result.statementType != tc.statement || result.dmlAffectedRows != tc.affectedRows {
			t.Fatalf("%s stats = type %q affected %d", tc.statement, result.statementType, result.dmlAffectedRows)
		}
		_, rows, ok := s.tables.getData("p1", "analytics", "items")
		if !ok || !equalStoredRows(rows, tc.wantRows) {
			t.Fatalf("%s catalog rows = %#v, want %#v", tc.statement, rows, tc.wantRows)
		}
	}
}

func TestPersistentDMLRecognizesLeadingCommentsAndQuotedProjectTarget(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)
	query := "-- dbt model: items\n/* trace_id=abc */ INSERT INTO `p1.analytics.items` (id, name, active) VALUES (3, 'three', TRUE)"
	stmt, handled, err := parsePersistentSQLStatement("p1", query)
	if err != nil || !handled || stmt.target.DatasetID != "analytics" || stmt.target.TableID != "items" {
		t.Fatalf("classification = %#v handled=%v err=%v", stmt, handled, err)
	}
	if _, err := s.executeQueryStatement("p1", "", query, "", "", nil); err != nil {
		t.Fatalf("commented quoted-project INSERT failed: %v", err)
	}
	_, rows, _ := s.tables.getData("p1", "analytics", "items")
	if len(rows) != 3 || rows[2][0] != "3" {
		t.Fatalf("commented INSERT rows = %#v", rows)
	}
}

func TestPersistentCreateTableAsSelectAndDrop(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)

	result, err := s.executeQueryStatement("p1", "", "CREATE TABLE analytics.active_items AS SELECT id, name FROM analytics.items WHERE active", "", "", nil)
	if err != nil {
		t.Fatalf("CTAS failed: %v", err)
	}
	if result.statementType != "CREATE_TABLE" {
		t.Fatalf("CTAS statement type = %q", result.statementType)
	}
	fields, rows, ok := s.tables.getData("p1", "analytics", "active_items")
	if !ok || len(fields) != 2 || !equalStoredRows(rows, [][]string{{"1", "one"}}) {
		t.Fatalf("unexpected CTAS table: fields=%#v rows=%#v ok=%v", fields, rows, ok)
	}

	if _, err := s.executeQueryStatement("p1", "", "DROP TABLE analytics.active_items", "", "", nil); err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}
	if _, _, ok := s.tables.getData("p1", "analytics", "active_items"); ok {
		t.Fatal("DROP TABLE did not remove the catalog resource")
	}
	if _, err := s.executeQueryStatement("p1", "", "DROP TABLE IF EXISTS analytics.active_items", "", "", nil); err != nil {
		t.Fatalf("DROP TABLE IF EXISTS should be idempotent: %v", err)
	}
}

func TestPersistentDMLFailureIsAtomic(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)
	_, before, _ := s.tables.getData("p1", "analytics", "items")

	if _, err := s.executeQueryStatement("p1", "", "UPDATE analytics.items SET id = NULL WHERE id = 1", "", "", nil); err == nil {
		t.Fatal("expected REQUIRED validation failure")
	}
	_, after, _ := s.tables.getData("p1", "analytics", "items")
	if !equalStoredRows(after, before) {
		t.Fatalf("failed DML changed catalog: before=%#v after=%#v", before, after)
	}
}

func TestUnsupportedMutatingStatementsFailExplicitly(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)
	for _, query := range []string{
		"ALTER TABLE analytics.items ADD COLUMN extra STRING",
		"CREATE VIEW analytics.item_view AS SELECT * FROM analytics.items",
		"CREATE TEMP TABLE temporary_items AS SELECT * FROM analytics.items",
		"DROP VIEW analytics.item_view",
	} {
		if _, err := s.executeQueryStatement("p1", "", query, "", "", nil); err == nil || !strings.Contains(err.Error(), "unsupported persistent SQL statement") {
			t.Fatalf("expected explicit unsupported mutation error for %q, got %v", query, err)
		}
	}
	fields, rows, ok := s.tables.getData("p1", "analytics", "items")
	if !ok || len(fields) != 3 || len(rows) != 2 {
		t.Fatalf("unsupported DDL changed catalog: fields=%#v rows=%#v", fields, rows)
	}
}

func TestPersistentMergeTruncateAndParameters(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)
	if _, created := s.tables.insert(tableInsert{
		ProjectID: "p1", DatasetID: "analytics", TableID: "incoming",
		Schema: []tableField{{Name: "id", Type: "INT64"}, {Name: "name", Type: "STRING"}, {Name: "active", Type: "BOOL"}},
		Rows:   [][]string{{"1", "merged", "false"}, {"3", "three", "true"}},
	}); !created {
		t.Fatal("incoming table was not created")
	}

	merge := `MERGE INTO analytics.items AS T
USING analytics.incoming AS S
ON T.id = S.id
WHEN MATCHED THEN UPDATE SET name = S.name, active = S.active
WHEN NOT MATCHED THEN INSERT (id, name, active) VALUES (S.id, S.name, S.active)`
	result, err := s.executeQueryStatement("p1", "", merge, "", "", nil)
	if err != nil {
		t.Fatalf("MERGE failed: %v", err)
	}
	if result.statementType != "MERGE" || result.dmlAffectedRows != 2 {
		t.Fatalf("MERGE stats = type %q affected %d", result.statementType, result.dmlAffectedRows)
	}
	_, rows, _ := s.tables.getData("p1", "analytics", "items")
	if !equalStoredRows(rows, [][]string{{"1", "merged", "false"}, {"2", storedNullCell, "false"}, {"3", "three", "true"}}) {
		t.Fatalf("MERGE rows = %#v", rows)
	}

	params := []storedQueryParameter{{Name: "id", Type: "INT64", Value: "4"}, {Name: "name", Type: "STRING", Value: "parameterized"}}
	result, err = s.executeQueryStatement("p1", "", "INSERT INTO analytics.items (id, name, active) VALUES (@id, @name, TRUE)", "", "NAMED", params)
	if err != nil {
		t.Fatalf("parameterized INSERT failed: %v", err)
	}
	if result.dmlAffectedRows != 1 {
		t.Fatalf("parameterized INSERT affected rows = %d", result.dmlAffectedRows)
	}

	result, err = s.executeQueryStatement("p1", "", "TRUNCATE TABLE analytics.items", "", "", nil)
	if err != nil {
		t.Fatalf("TRUNCATE TABLE failed: %v", err)
	}
	_, rows, _ = s.tables.getData("p1", "analytics", "items")
	if len(rows) != 0 || result.statementType != "TRUNCATE_TABLE" || result.dmlAffectedRows != 4 {
		t.Fatalf("TRUNCATE result=%#v rows=%#v", result, rows)
	}
}

func TestPersistentCreateTableSchemaAndReplace(t *testing.T) {
	s := newTestServer()
	if !s.datasets.exists("p1", "analytics") {
		t.Fatal("expected analytics dataset")
	}
	if _, err := s.executeQueryStatement("p1", "", "CREATE TABLE analytics.created (id INT64, name STRING)", "", "", nil); err != nil {
		t.Fatalf("CREATE TABLE schema failed: %v", err)
	}
	fields, rows, ok := s.tables.getData("p1", "analytics", "created")
	if !ok || len(fields) != 2 || len(rows) != 0 {
		t.Fatalf("created table = fields %#v rows %#v ok %v", fields, rows, ok)
	}
	if _, err := s.executeQueryStatement("p1", "", "CREATE OR REPLACE TABLE analytics.created AS SELECT 7 AS id, 'seven' AS name", "", "", nil); err != nil {
		t.Fatalf("CREATE OR REPLACE TABLE failed: %v", err)
	}
	_, rows, _ = s.tables.getData("p1", "analytics", "created")
	if !equalStoredRows(rows, [][]string{{"7", "seven"}}) {
		t.Fatalf("replaced table rows = %#v", rows)
	}
}

func TestPersistentDMLJobStatisticsAndResourceLock(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)
	body := `{"query":"INSERT INTO analytics.items (id, name, active) VALUES (3, 'three', TRUE)","timeoutMs":5000}`
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/queries", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("jobs.query status %d: %s", res.Code, res.Body.String())
	}
	var queryResponse map[string]any
	if err := json.NewDecoder(res.Body).Decode(&queryResponse); err != nil {
		t.Fatal(err)
	}
	if queryResponse["numDmlAffectedRows"] != "1" {
		t.Fatalf("query response numDmlAffectedRows = %v", queryResponse["numDmlAffectedRows"])
	}
	jobID := queryResponse["jobReference"].(map[string]any)["jobId"].(string)
	job, ok := s.jobs.get("p1", jobID)
	if !ok {
		t.Fatal("query job not found")
	}
	if job.ResourceKey != "p1:analytics.items" {
		t.Fatalf("resource key = %q", job.ResourceKey)
	}
	resource := renderJobResource(job)
	queryStats := resource["statistics"].(map[string]any)["query"].(map[string]any)
	if queryStats["statementType"] != "INSERT" || queryStats["numDmlAffectedRows"] != "1" {
		t.Fatalf("unexpected query statistics: %#v", queryStats)
	}
	dmlStats := queryStats["dmlStats"].(map[string]string)
	if dmlStats["insertedRowCount"] != "1" || dmlStats["updatedRowCount"] != "0" || dmlStats["deletedRowCount"] != "0" {
		t.Fatalf("unexpected INSERT dmlStats: %#v", dmlStats)
	}
	if time.Since(job.EndedAt) < 0 {
		t.Fatal("job completion timestamp is in the future")
	}
}

func TestPollingPendingDMLDoesNotExecuteItEarlyOrTwice(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)
	body := `{"configuration":{"query":{"query":"INSERT INTO analytics.items (id, name, active) VALUES (3, 'three', TRUE)","priority":"BATCH"}}}`
	createReq := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/jobs", strings.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusCreated {
		t.Fatalf("create status %d: %s", createRes.Code, createRes.Body.String())
	}
	var created map[string]any
	if err := json.NewDecoder(createRes.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	jobID := created["jobReference"].(map[string]any)["jobId"].(string)

	pollReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs/"+jobID+"/queryResults", nil)
	pollRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(pollRes, pollReq)
	if pollRes.Code != http.StatusOK {
		t.Fatalf("early poll status %d: %s", pollRes.Code, pollRes.Body.String())
	}
	var early map[string]any
	if err := json.NewDecoder(pollRes.Body).Decode(&early); err != nil {
		t.Fatal(err)
	}
	if early["jobComplete"] != false || len(early["rows"].([]any)) != 0 {
		t.Fatalf("early poll unexpectedly completed/executed DML: %#v", early)
	}
	_, rows, _ := s.tables.getData("p1", "analytics", "items")
	if len(rows) != 2 {
		t.Fatalf("pending DML changed catalog early: %#v", rows)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, _ := s.jobs.get("p1", jobID)
		if job != nil && job.State == jobStateDone {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	job, _ := s.jobs.get("p1", jobID)
	if job == nil || job.State != jobStateDone || job.ErrorReason != "" {
		t.Fatalf("DML job did not complete successfully: %#v", job)
	}
	_, rows, _ = s.tables.getData("p1", "analytics", "items")
	if len(rows) != 3 || rows[2][0] != "3" {
		t.Fatalf("DML should execute exactly once: %#v", rows)
	}
}

func equalStoredRows(left, right [][]string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if len(left[i]) != len(right[i]) {
			return false
		}
		for j := range left[i] {
			if left[i][j] != right[i][j] {
				return false
			}
		}
	}
	return true
}
