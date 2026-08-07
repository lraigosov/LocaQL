package server

import (
	"testing"
	"time"
)

func TestTimePartitionEqualityPrunesSourceBytes(t *testing.T) {
	s := newTestServer()
	createPartitionedTable(t, s, "prune_daily", `{
  "tableReference":{"tableId":"prune_daily"},
  "schema":{"fields":[{"name":"event_date","type":"DATE"},{"name":"user_id","type":"STRING"}]},
  "timePartitioning":{"type":"DAY","field":"event_date"},
  "requirePartitionFilter":true
}`)
	table, _, _ := s.tables.get("p1", "analytics", "prune_daily")
	rows := [][]string{{"2026-08-05", "a"}, {"2026-08-05", "b"}, {"2026-08-06", "c"}}
	if _, err := s.tables.upsertCopyDestination(tableReference{ProjectID: "p1", DatasetID: "analytics", TableID: "prune_daily"}, table.Schema, rows, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append daily rows: %v", err)
	}

	result, err := s.executeQueryStatement("p1", "", "SELECT user_id FROM analytics.prune_daily WHERE event_date = DATE '2026-08-05' AND user_id != 'never' ORDER BY user_id", "", "", nil)
	if err != nil {
		t.Fatalf("query pruned daily partition: %v", err)
	}
	if len(result.rows) != 2 || result.rows[0][0] != "a" || result.rows[1][0] != "b" {
		t.Fatalf("unexpected pruned query rows: %v", result.rows)
	}
	wantBytes := estimateRowsByteSize(rows[:2])
	if result.processedBytes != wantBytes {
		t.Fatalf("expected %d bytes from one daily partition, got %d", wantBytes, result.processedBytes)
	}
}

func TestPartitionPruningDoesNotNarrowOrPredicate(t *testing.T) {
	s := newTestServer()
	createPartitionedTable(t, s, "prune_or", `{
  "tableReference":{"tableId":"prune_or"},
  "schema":{"fields":[{"name":"event_date","type":"DATE"},{"name":"user_id","type":"STRING"}]},
  "timePartitioning":{"type":"DAY","field":"event_date"}
}`)
	table, _, _ := s.tables.get("p1", "analytics", "prune_or")
	rows := [][]string{{"2026-08-05", "a"}, {"2026-08-06", "b"}, {"2026-08-07", "keep-from-other-partition"}}
	if _, err := s.tables.upsertCopyDestination(tableReference{ProjectID: "p1", DatasetID: "analytics", TableID: "prune_or"}, table.Schema, rows, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append OR rows: %v", err)
	}

	result, err := s.executeQueryStatement("p1", "", "SELECT user_id FROM analytics.prune_or WHERE event_date = DATE '2026-08-05' OR user_id = 'keep-from-other-partition' ORDER BY user_id", "", "", nil)
	if err != nil {
		t.Fatalf("query OR predicate: %v", err)
	}
	if len(result.rows) != 2 {
		t.Fatalf("OR predicate must retain a row from another partition: %v", result.rows)
	}
	if result.processedBytes != estimateRowsByteSize(rows) {
		t.Fatalf("OR predicate must scan the full source, got %d", result.processedBytes)
	}
}

func TestRangePartitionEqualityScansWholeBucket(t *testing.T) {
	s := newTestServer()
	createPartitionedTable(t, s, "prune_range", `{
  "tableReference":{"tableId":"prune_range"},
  "schema":{"fields":[{"name":"bucket_id","type":"INT64"},{"name":"label","type":"STRING"}]},
  "rangePartitioning":{"field":"bucket_id","range":{"start":"0","end":"100","interval":"10"}}
}`)
	table, _, _ := s.tables.get("p1", "analytics", "prune_range")
	rows := [][]string{{"0", "zero"}, {"9", "nine"}, {"10", "ten"}, {"99", "max"}}
	if _, err := s.tables.upsertCopyDestination(tableReference{ProjectID: "p1", DatasetID: "analytics", TableID: "prune_range"}, table.Schema, rows, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append range rows: %v", err)
	}

	result, err := s.executeQueryStatement("p1", "", "SELECT label FROM analytics.prune_range WHERE bucket_id = 9", "", "", nil)
	if err != nil {
		t.Fatalf("query range partition: %v", err)
	}
	if len(result.rows) != 1 || result.rows[0][0] != "nine" {
		t.Fatalf("unexpected range result: %v", result.rows)
	}
	if result.processedBytes != estimateRowsByteSize(rows[:2]) {
		t.Fatalf("range equality must scan its whole [0,10) bucket, got %d", result.processedBytes)
	}
}

func TestIngestionPartitionEqualityPrunesRowsAndKeepsPseudocolumnsAligned(t *testing.T) {
	s := newTestServer()
	clock := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	s.tables.now = func() time.Time { return clock }
	createPartitionedTable(t, s, "prune_ingestion", `{
  "tableReference":{"tableId":"prune_ingestion"},
  "schema":{"fields":[{"name":"id","type":"INT64"}]},
  "timePartitioning":{"type":"DAY"}
}`)
	destination := tableReference{ProjectID: "p1", DatasetID: "analytics", TableID: "prune_ingestion"}
	table, _, _ := s.tables.get("p1", "analytics", "prune_ingestion")
	if _, err := s.tables.upsertCopyDestination(destination, table.Schema, [][]string{{"1"}}, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append first ingestion row: %v", err)
	}
	clock = clock.Add(24 * time.Hour)
	if _, err := s.tables.upsertCopyDestination(destination, table.Schema, [][]string{{"2"}, {"3"}}, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append second ingestion partition: %v", err)
	}

	result, err := s.executeQueryStatement("p1", "", "SELECT _PARTITIONDATE, id FROM analytics.prune_ingestion WHERE _PARTITIONDATE = DATE '2026-08-05'", "", "", nil)
	if err != nil {
		t.Fatalf("query ingestion partition: %v", err)
	}
	if len(result.rows) != 1 || result.rows[0][0] != "2026-08-05" || result.rows[0][1] != "1" {
		t.Fatalf("pseudocolumn must remain aligned after pruning: %v", result.rows)
	}
	if result.processedBytes != int64(len("1")) {
		t.Fatalf("expected only first ingestion row byte, got %d", result.processedBytes)
	}
}

func TestPersistentDMLNeverUsesReadOnlyPartitionPruning(t *testing.T) {
	s := newTestServer()
	createPartitionedTable(t, s, "no_prune_dml", `{
  "tableReference":{"tableId":"no_prune_dml"},
  "schema":{"fields":[{"name":"event_date","type":"DATE"},{"name":"value","type":"INT64"}]},
  "timePartitioning":{"type":"DAY","field":"event_date"}
}`)
	table, _, _ := s.tables.get("p1", "analytics", "no_prune_dml")
	rows := [][]string{{"2026-08-05", "1"}, {"2026-08-06", "2"}}
	if _, err := s.tables.upsertCopyDestination(tableReference{ProjectID: "p1", DatasetID: "analytics", TableID: "no_prune_dml"}, table.Schema, rows, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append DML rows: %v", err)
	}

	result, err := s.executeQueryStatement("p1", "", "UPDATE analytics.no_prune_dml SET value = 10 WHERE event_date = DATE '2026-08-05'", "", "", nil)
	if err != nil {
		t.Fatalf("update partitioned table: %v", err)
	}
	if result.processedBytes != estimateRowsByteSize(rows) {
		t.Fatalf("DML must materialize the complete target, got %d bytes", result.processedBytes)
	}
	table, _, _ = s.tables.get("p1", "analytics", "no_prune_dml")
	if len(table.Rows) != 2 || table.Rows[0][1] != "10" || table.Rows[1][1] != "2" {
		t.Fatalf("DML must preserve the untouched partition: %v", table.Rows)
	}
}

func TestPartitionFilterASTIgnoresColumnNamesInLiteralsAndComments(t *testing.T) {
	table := &tableRecord{
		Schema:                 []tableField{{Name: "event_date", Type: "DATE"}, {Name: "note", Type: "STRING"}},
		TimePartitioning:       &timePartitioningConfig{Type: "DAY", Field: "event_date"},
		RequirePartitionFilter: true,
	}
	query := "SELECT * FROM analytics.events WHERE note = 'event_date' -- event_date"
	if queryHasPartitionFilter(query, table) {
		t.Fatal("partition column text inside a literal/comment must not satisfy requirePartitionFilter")
	}
}
