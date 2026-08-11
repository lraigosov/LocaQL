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
		"ALTER TABLE analytics.items ALTER COLUMN active SET DATA TYPE STRING",
		"ALTER TABLE analytics.items ADD COLUMN a INT64, DROP COLUMN active",
		"CREATE TEMP TABLE temporary_items AS SELECT * FROM analytics.items",
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

// TestPersistentCreateSchema covers CREATE SCHEMA [IF NOT EXISTS] — real
// BigQuery's SQL spelling of dataset creation, which SQLMesh's own
// state-store bootstrap issues before this project supported it.
func TestPersistentCreateSchema(t *testing.T) {
	s := newTestServer()
	if s.datasets.exists("p1", "new_ds") {
		t.Fatal("dataset should not exist yet")
	}
	if _, err := s.executeQueryStatement("p1", "", "CREATE SCHEMA new_ds", "", "", nil); err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}
	if !s.datasets.exists("p1", "new_ds") {
		t.Fatal("CREATE SCHEMA did not create the dataset")
	}
	if _, err := s.executeQueryStatement("p1", "", "CREATE SCHEMA new_ds", "", "", nil); err == nil {
		t.Fatal("expected CREATE SCHEMA without IF NOT EXISTS to fail on an existing dataset")
	}
	if _, err := s.executeQueryStatement("p1", "", "CREATE SCHEMA IF NOT EXISTS new_ds", "", "", nil); err != nil {
		t.Fatalf("CREATE SCHEMA IF NOT EXISTS should tolerate an existing dataset, got: %v", err)
	}
}

// TestPersistentAlterTableAddColumn covers ALTER TABLE ADD COLUMN, real
// BigQuery schema-evolution DDL that SQLMesh's own migration framework
// issues against its state tables and that dbt's
// "on_schema_change: append_new_columns" issues against user tables.
func TestPersistentAlterTableAddColumn(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)

	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items ADD COLUMN score FLOAT64", "", "", nil); err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN failed: %v", err)
	}
	fields, rows, ok := s.tables.getData("p1", "analytics", "items")
	if !ok || len(fields) != 4 || fields[3].Name != "score" || fields[3].Type != "FLOAT64" {
		t.Fatalf("expected a new nullable score column, got fields %#v", fields)
	}
	if !equalStoredRows(rows, [][]string{{"1", "one", "true", storedNullCell}, {"2", storedNullCell, "false", storedNullCell}}) {
		t.Fatalf("existing rows should get NULL for the new column, got %#v", rows)
	}

	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items ADD COLUMN score INT64", "", "", nil); err == nil {
		t.Fatal("expected ADD COLUMN to reject a name that already exists")
	}
	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items ADD COLUMN IF NOT EXISTS score INT64", "", "", nil); err != nil {
		t.Fatalf("ADD COLUMN IF NOT EXISTS should tolerate an existing column, got: %v", err)
	}
	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE IF EXISTS analytics.does_not_exist ADD COLUMN x INT64", "", "", nil); err != nil {
		t.Fatalf("ALTER TABLE IF EXISTS on a missing table should be a no-op, got: %v", err)
	}
	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.does_not_exist ADD COLUMN x INT64", "", "", nil); err == nil {
		t.Fatal("expected ALTER TABLE without IF EXISTS to fail on a missing table")
	}

	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items ADD COLUMN a BOOLEAN, ADD COLUMN b STRING", "", "", nil); err != nil {
		t.Fatalf("multi-clause ADD COLUMN failed: %v", err)
	}
	fields, _, _ = s.tables.getData("p1", "analytics", "items")
	if len(fields) != 6 || fields[4].Name != "a" || fields[4].Type != "BOOL" || fields[5].Name != "b" || fields[5].Type != "STRING" {
		t.Fatalf("expected two more nullable columns with normalized types, got %#v", fields)
	}
}

func TestPersistentAlterTableDropColumn(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)

	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items DROP COLUMN active", "", "", nil); err != nil {
		t.Fatalf("ALTER TABLE DROP COLUMN failed: %v", err)
	}
	fields, rows, ok := s.tables.getData("p1", "analytics", "items")
	if !ok || len(fields) != 2 || fields[0].Name != "id" || fields[1].Name != "name" {
		t.Fatalf("expected active column gone, got fields %#v", fields)
	}
	if !equalStoredRows(rows, [][]string{{"1", "one"}, {"2", storedNullCell}}) {
		t.Fatalf("expected the active cell dropped from every row, got %#v", rows)
	}

	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items DROP COLUMN active", "", "", nil); err == nil {
		t.Fatal("expected DROP COLUMN to reject a name that no longer exists")
	}
	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items DROP COLUMN IF EXISTS active", "", "", nil); err != nil {
		t.Fatalf("DROP COLUMN IF EXISTS should tolerate an already-missing column, got: %v", err)
	}

	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items DROP COLUMN id, DROP COLUMN name", "", "", nil); err != nil {
		t.Fatalf("multi-clause DROP COLUMN failed: %v", err)
	}
	fields, rows, _ = s.tables.getData("p1", "analytics", "items")
	if len(fields) != 0 || len(rows) != 2 || len(rows[0]) != 0 {
		t.Fatalf("expected every column gone but row count preserved, got fields=%#v rows=%#v", fields, rows)
	}
}

func TestPersistentAlterTableRenameColumn(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)

	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items RENAME COLUMN name TO label", "", "", nil); err != nil {
		t.Fatalf("ALTER TABLE RENAME COLUMN failed: %v", err)
	}
	fields, rows, ok := s.tables.getData("p1", "analytics", "items")
	if !ok || fields[1].Name != "label" {
		t.Fatalf("expected name renamed to label, got fields %#v", fields)
	}
	if !equalStoredRows(rows, [][]string{{"1", "one", "true"}, {"2", storedNullCell, "false"}}) {
		t.Fatalf("renaming a column should never move data, got %#v", rows)
	}

	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items RENAME COLUMN active TO label", "", "", nil); err == nil {
		t.Fatal("expected RENAME COLUMN to reject a new name that already exists")
	}
	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items RENAME COLUMN does_not_exist TO x", "", "", nil); err == nil {
		t.Fatal("expected RENAME COLUMN to reject an old name that does not exist")
	}
	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items RENAME COLUMN IF EXISTS does_not_exist TO x", "", "", nil); err != nil {
		t.Fatalf("RENAME COLUMN IF EXISTS should tolerate a missing old name, got: %v", err)
	}
}

func TestPersistentAlterTableRenameTo(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)

	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.items RENAME TO renamed_items", "", "", nil); err != nil {
		t.Fatalf("ALTER TABLE RENAME TO failed: %v", err)
	}
	if _, ok, _ := s.tables.get("p1", "analytics", "items"); ok {
		t.Fatal("old table name should no longer exist after RENAME TO")
	}
	fields, rows, ok := s.tables.getData("p1", "analytics", "renamed_items")
	if !ok || len(fields) != 3 {
		t.Fatalf("expected the table to exist under its new name with its schema intact, got fields=%#v ok=%v", fields, ok)
	}
	if !equalStoredRows(rows, [][]string{{"1", "one", "true"}, {"2", storedNullCell, "false"}}) {
		t.Fatalf("RENAME TO should never change row data, got %#v", rows)
	}

	if _, err := s.executeQueryStatement("p1", "", "CREATE TABLE analytics.other (id INT64)", "", "", nil); err != nil {
		t.Fatalf("seed second table failed: %v", err)
	}
	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.renamed_items RENAME TO other", "", "", nil); err == nil {
		t.Fatal("expected RENAME TO to reject a name that already exists")
	}
	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE IF EXISTS analytics.does_not_exist RENAME TO whatever", "", "", nil); err != nil {
		t.Fatalf("ALTER TABLE IF EXISTS RENAME TO on a missing table should be a no-op, got: %v", err)
	}
}

func TestPersistentAlterTableSetOptions(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)

	if _, err := s.executeQueryStatement("p1", "", `ALTER TABLE analytics.items SET OPTIONS(description="updated via ALTER TABLE")`, "", "", nil); err != nil {
		t.Fatalf("ALTER TABLE SET OPTIONS failed: %v", err)
	}
	fields, rows, ok := s.tables.getData("p1", "analytics", "items")
	if !ok || len(fields) != 3 || len(rows) != 2 {
		t.Fatalf("SET OPTIONS should never change schema/rows, got fields=%#v rows=%#v", fields, rows)
	}

	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE analytics.does_not_exist SET OPTIONS(description=\"x\")", "", "", nil); err == nil {
		t.Fatal("expected SET OPTIONS to fail on a missing table")
	}
	if _, err := s.executeQueryStatement("p1", "", "ALTER TABLE IF EXISTS analytics.does_not_exist SET OPTIONS(description=\"x\")", "", "", nil); err != nil {
		t.Fatalf("ALTER TABLE IF EXISTS SET OPTIONS on a missing table should be a no-op, got: %v", err)
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

func TestPersistentViewCreateReplaceDrop(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)

	if _, err := s.executeQueryStatement("p1", "", "CREATE VIEW analytics.item_view AS SELECT id, name FROM analytics.items", "", "", nil); err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}
	tbl, ok, _ := s.tables.get("p1", "analytics", "item_view")
	if !ok || tbl.View == nil || tbl.View.Materialized {
		t.Fatalf("expected a non-materialized view, got %#v", tbl)
	}
	if len(tbl.Schema) != 2 {
		t.Fatalf("expected derived 2-column schema, got %#v", tbl.Schema)
	}

	if _, err := s.executeQueryStatement("p1", "", "CREATE VIEW analytics.item_view AS SELECT id FROM analytics.items", "", "", nil); err == nil {
		t.Fatal("expected CREATE VIEW without OR REPLACE to fail on an existing name")
	}

	if _, err := s.executeQueryStatement("p1", "", "CREATE OR REPLACE MATERIALIZED VIEW analytics.item_view AS SELECT id FROM analytics.items", "", "", nil); err != nil {
		t.Fatalf("CREATE OR REPLACE MATERIALIZED VIEW failed: %v", err)
	}
	tbl, ok, _ = s.tables.get("p1", "analytics", "item_view")
	if !ok || tbl.View == nil || !tbl.View.Materialized || len(tbl.Schema) != 1 {
		t.Fatalf("expected replaced materialized 1-column view, got %#v", tbl)
	}

	if _, err := s.executeQueryStatement("p1", "", "DROP VIEW IF EXISTS analytics.does_not_exist", "", "", nil); err != nil {
		t.Fatalf("DROP VIEW IF EXISTS on a missing view should be a no-op, got: %v", err)
	}
	if _, err := s.executeQueryStatement("p1", "", "DROP TABLE analytics.item_view", "", "", nil); err == nil {
		t.Fatal("expected DROP TABLE to reject a view target")
	}
	if _, err := s.executeQueryStatement("p1", "", "DROP MATERIALIZED VIEW analytics.item_view", "", "", nil); err != nil {
		t.Fatalf("DROP MATERIALIZED VIEW failed: %v", err)
	}
	if _, ok, _ := s.tables.get("p1", "analytics", "item_view"); ok {
		t.Fatal("view still present after DROP")
	}
}

// TestPersistentTargetAcceptsPerSegmentBacktickIdentifiers guards against a
// real bug found running dbt-bigquery's default table/incremental DDL
// macros: they quote each identifier segment individually
// (a backtick around each of project, dataset and table individually),
// which the target regex originally
// only accepted as a single pair of backticks around the whole dotted name.
func TestPersistentTargetAcceptsPerSegmentBacktickIdentifiers(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)

	if _, err := s.executeQueryStatement("p1", "", "CREATE OR REPLACE TABLE `p1`.`analytics`.`per_seg` AS SELECT 1 AS id", "", "", nil); err != nil {
		t.Fatalf("per-segment backtick CREATE TABLE failed: %v", err)
	}
	if _, ok, _ := s.tables.get("p1", "analytics", "per_seg"); !ok {
		t.Fatal("per-segment backtick target did not create the table")
	}
	if _, err := s.executeQueryStatement("p1", "", "INSERT INTO `analytics`.`per_seg` (id) VALUES (2)", "", "", nil); err != nil {
		t.Fatalf("per-segment backtick INSERT (dataset.table form) failed: %v", err)
	}
	_, rows, _ := s.tables.getData("p1", "analytics", "per_seg")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after per-segment-backtick insert, got %#v", rows)
	}
}

// TestPollingPendingCreateViewDoesNotExecuteItEarlyOrTwice mirrors
// TestPollingPendingDMLDoesNotExecuteItEarlyOrTwice for CREATE VIEW: a real
// bug found running dbt-bigquery's default view materialization meant
// computeQueryJobResultRows only recognized parsePersistentSQLStatement's
// mutating statements, missing parsePersistentViewStatement entirely, so a
// getQueryResults poll landing while the async job was still PENDING/RUNNING
// re-ran the CREATE VIEW a second time, racing the job's own execution over
// the same catalog version.
func TestPollingPendingCreateViewDoesNotExecuteItEarlyOrTwice(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)
	body := `{"configuration":{"query":{"query":"CREATE VIEW analytics.item_view AS SELECT id, name FROM analytics.items","priority":"BATCH"}}}`
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
	if early["jobComplete"] != false {
		t.Fatalf("early poll unexpectedly reported completion: %#v", early)
	}
	if _, ok, _ := s.tables.get("p1", "analytics", "item_view"); ok {
		t.Fatal("pending CREATE VIEW ran early against the catalog")
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
		t.Fatalf("CREATE VIEW job did not complete successfully: %#v", job)
	}
	if _, ok, _ := s.tables.get("p1", "analytics", "item_view"); !ok {
		t.Fatal("CREATE VIEW job finished without creating the view")
	}

	// A second, post-completion poll must keep returning the same
	// already-computed result rather than re-running CREATE VIEW again
	// (which would now fail with "table already exists").
	secondPollRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(secondPollRes, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p1/jobs/"+jobID+"/queryResults", nil))
	if secondPollRes.Code != http.StatusOK {
		t.Fatalf("post-completion poll status %d: %s", secondPollRes.Code, secondPollRes.Body.String())
	}
}

// TestPersistentMergeUsingSubqueryDerivedTable exercises
// rewriteMergeUsingSubquery: the embedded GoogleSQL engine's MERGE only
// accepts a bare table reference as its USING source (see that function's
// doc comment), but dbt-bigquery's default incremental "merge" strategy
// always emits USING (<select>) AS DBT_INTERNAL_SOURCE. Without the
// materialize-and-rewrite workaround this fails with "MERGE: source must be
// a single-table reference".
func TestPersistentMergeUsingSubqueryDerivedTable(t *testing.T) {
	s := newTestServer()
	seedPersistentSQLTable(t, s)

	merge := `MERGE INTO analytics.items AS T
USING (SELECT 1 AS id, 'merged' AS name, FALSE AS active UNION ALL SELECT 4 AS id, 'four' AS name, TRUE AS active) AS S
ON T.id = S.id
WHEN MATCHED THEN UPDATE SET name = S.name, active = S.active
WHEN NOT MATCHED THEN INSERT (id, name, active) VALUES (S.id, S.name, S.active)`
	result, err := s.executeQueryStatement("p1", "", merge, "", "", nil)
	if err != nil {
		t.Fatalf("MERGE with a derived-table USING source failed: %v", err)
	}
	if result.statementType != "MERGE" || result.dmlAffectedRows != 2 {
		t.Fatalf("MERGE stats = type %q affected %d", result.statementType, result.dmlAffectedRows)
	}
	_, rows, _ := s.tables.getData("p1", "analytics", "items")
	if !equalStoredRows(rows, [][]string{{"1", "merged", "false"}, {"2", storedNullCell, "false"}, {"4", "four", "true"}}) {
		t.Fatalf("MERGE rows = %#v", rows)
	}

	for _, tbl := range s.tables.listAll("p1", "analytics") {
		if strings.HasPrefix(tbl.TableID, "__locaql_merge_src_") {
			t.Fatalf("ephemeral MERGE source table %q was not cleaned up", tbl.TableID)
		}
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
