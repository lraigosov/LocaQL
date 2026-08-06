package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func createPartitionedTable(t *testing.T, s *Server, tableID, body string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("create table %s returned %d: %s", tableID, res.Code, res.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode create table %s: %v", tableID, err)
	}
	return out
}

func TestTablePartitioningMetadataRoundTripPatchAndValidation(t *testing.T) {
	s := newTestServer()
	body := `{
  "tableReference":{"tableId":"events_partitioned"},
  "schema":{"fields":[
    {"name":"event_date","type":"DATE"},
    {"name":"user_id","type":"STRING"},
    {"name":"score","type":"INT64"}
  ]},
  "timePartitioning":{"type":"DAY","field":"event_date","expirationMs":"86400000"},
  "clustering":{"fields":["user_id","score"]},
  "requirePartitionFilter":true
}`
	out := createPartitionedTable(t, s, "events_partitioned", body)
	timeConfig := out["timePartitioning"].(map[string]any)
	if timeConfig["type"] != "DAY" || timeConfig["field"] != "event_date" || timeConfig["expirationMs"] != "86400000" {
		t.Fatalf("unexpected timePartitioning: %v", timeConfig)
	}
	clusterFields := out["clustering"].(map[string]any)["fields"].([]any)
	if len(clusterFields) != 2 || clusterFields[0] != "user_id" || clusterFields[1] != "score" {
		t.Fatalf("unexpected clustering fields: %v", clusterFields)
	}
	if out["requirePartitionFilter"] != true {
		t.Fatalf("expected requirePartitionFilter=true, got %v", out["requirePartitionFilter"])
	}

	patchBody := `{"timePartitioning":{"type":"DAY","field":"event_date","expirationMs":"172800000"},"clustering":{"fields":["score"]}}`
	patchReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/analytics/tables/events_partitioned", strings.NewReader(patchBody))
	patchReq.Header.Set("Content-Type", "application/json")
	patchRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(patchRes, patchReq)
	if patchRes.Code != http.StatusOK {
		t.Fatalf("patch partition metadata returned %d: %s", patchRes.Code, patchRes.Body.String())
	}
	var patched map[string]any
	if err := json.NewDecoder(patchRes.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	if patched["timePartitioning"].(map[string]any)["expirationMs"] != "172800000" {
		t.Fatalf("expected updated partition expiration: %v", patched["timePartitioning"])
	}
	patchedClusters := patched["clustering"].(map[string]any)["fields"].([]any)
	if len(patchedClusters) != 1 || patchedClusters[0] != "score" {
		t.Fatalf("expected clustering replacement, got %v", patchedClusters)
	}

	immutableReq := httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/p1/datasets/analytics/tables/events_partitioned", strings.NewReader(`{"timePartitioning":{"type":"MONTH","field":"event_date"}}`))
	immutableReq.Header.Set("Content-Type", "application/json")
	immutableRes := httptest.NewRecorder()
	s.Handler().ServeHTTP(immutableRes, immutableReq)
	if immutableRes.Code != http.StatusBadRequest || !strings.Contains(immutableRes.Body.String(), "immutable") {
		t.Fatalf("expected explicit immutable partitioning error, got %d: %s", immutableRes.Code, immutableRes.Body.String())
	}

	invalidCases := []string{
		`{"tableReference":{"tableId":"invalid_both"},"schema":{"fields":[{"name":"d","type":"DATE"},{"name":"id","type":"INT64"}]},"timePartitioning":{"field":"d"},"rangePartitioning":{"field":"id","range":{"start":"0","end":"10","interval":"1"}}}`,
		`{"tableReference":{"tableId":"invalid_time_type"},"schema":{"fields":[{"name":"name","type":"STRING"}]},"timePartitioning":{"field":"name"}}`,
		`{"tableReference":{"tableId":"invalid_cluster"},"schema":{"fields":[{"name":"id","type":"INT64"}]},"clustering":{"fields":["missing"]}}`,
		`{"tableReference":{"tableId":"invalid_filter"},"schema":{"fields":[{"name":"id","type":"INT64"}]},"requirePartitionFilter":true}`,
		`{"tableReference":{"tableId":"invalid_ingestion_filter"},"schema":{"fields":[{"name":"id","type":"INT64"}]},"timePartitioning":{"type":"DAY"},"requirePartitionFilter":true}`,
	}
	for _, invalidBody := range invalidCases {
		req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p1/datasets/analytics/tables", strings.NewReader(invalidBody))
		req.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		s.Handler().ServeHTTP(res, req)
		if res.Code != http.StatusBadRequest {
			t.Fatalf("expected invalid partition/clustering request to return 400, got %d: %s", res.Code, res.Body.String())
		}
	}
}

func TestTimePartitionExpirationPurgesRowsLazily(t *testing.T) {
	s := newTestServer()
	clock := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	s.tables.now = func() time.Time { return clock }
	createPartitionedTable(t, s, "expiring_partitions", `{
  "tableReference":{"tableId":"expiring_partitions"},
  "schema":{"fields":[{"name":"event_date","type":"DATE"},{"name":"id","type":"INT64"}]},
  "timePartitioning":{"type":"DAY","field":"event_date","expirationMs":"86400000"}
}`)
	record, _, _ := s.tables.get("p1", "analytics", "expiring_partitions")
	rows := [][]string{{"2026-08-04", "1"}, {"2026-08-06", "2"}, {storedNullCell, "3"}}
	if _, err := s.tables.upsertCopyDestination(tableReference{ProjectID: "p1", DatasetID: "analytics", TableID: "expiring_partitions"}, record.Schema, rows, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append rows for partition expiration: %v", err)
	}
	record, ok, _ := s.tables.get("p1", "analytics", "expiring_partitions")
	if !ok {
		t.Fatal("expiring partition table disappeared")
	}
	if len(record.Rows) != 2 || record.Rows[0][0] != "2026-08-06" || record.Rows[1][0] != storedNullCell {
		t.Fatalf("expected only expired dated partition removed and NULL retained, got %v", record.Rows)
	}
}

func TestTimePartitioningInformationSchemaAndRequiredFilter(t *testing.T) {
	s := newTestServer()
	createPartitionedTable(t, s, "daily_events", `{
  "tableReference":{"tableId":"daily_events"},
  "schema":{"fields":[{"name":"event_date","type":"DATE"},{"name":"user_id","type":"STRING"}]},
  "timePartitioning":{"type":"DAY","field":"event_date","expirationMs":"172800000"},
  "clustering":{"fields":["user_id"]},
  "requirePartitionFilter":true
}`)
	record, ok, _ := s.tables.get("p1", "analytics", "daily_events")
	if !ok {
		t.Fatal("partitioned table not found")
	}
	rows := [][]string{{"2026-08-05", "a"}, {"2026-08-05", "b"}, {"2026-08-06", "c"}, {storedNullCell, "n"}}
	if _, err := s.tables.upsertCopyDestination(tableReference{ProjectID: "p1", DatasetID: "analytics", TableID: "daily_events"}, record.Schema, rows, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append partition rows: %v", err)
	}

	code, out := syncQuery(t, s, "SELECT user_id FROM analytics.daily_events")
	errorJSON, _ := json.Marshal(out)
	if code != http.StatusBadRequest || !strings.Contains(strings.ToLower(string(errorJSON)), "partition") {
		t.Fatalf("expected missing partition filter error, got %d: %v", code, out)
	}
	code, out = syncQuery(t, s, "SELECT user_id FROM analytics.daily_events WHERE event_date >= DATE '2026-08-05' ORDER BY user_id")
	if code != http.StatusOK || len(out["rows"].([]any)) != 3 {
		t.Fatalf("expected filtered query to return 3 rows, got %d: %v", code, out)
	}

	_, partitionRows, handled := s.simulateInformationSchemaQuery("p1", "SELECT * FROM analytics.INFORMATION_SCHEMA.PARTITIONS", "select * from analytics.information_schema.partitions", "")
	if !handled {
		t.Fatal("PARTITIONS query was not handled")
	}
	counts := map[string]string{}
	for _, row := range partitionRows {
		if row[2] == "daily_events" {
			counts[row[3]] = row[4]
		}
	}
	if counts["20260805"] != "2" || counts["20260806"] != "1" || counts["__NULL__"] != "1" {
		t.Fatalf("unexpected time partition counts: %v", counts)
	}

	_, columnRows, _ := s.simulateInformationSchemaQuery("p1", "SELECT * FROM analytics.INFORMATION_SCHEMA.COLUMNS", "select * from analytics.information_schema.columns", "")
	foundPartition, foundCluster := false, false
	for _, row := range columnRows {
		if row[2] != "daily_events" {
			continue
		}
		if row[3] == "event_date" && row[6] == "YES" {
			foundPartition = true
		}
		if row[3] == "user_id" && row[7] == "1" {
			foundCluster = true
		}
	}
	if !foundPartition || !foundCluster {
		t.Fatalf("expected partition/clustering column metadata, partition=%t cluster=%t", foundPartition, foundCluster)
	}

	_, optionRows, _ := s.simulateInformationSchemaQuery("p1", "SELECT * FROM analytics.INFORMATION_SCHEMA.TABLE_OPTIONS", "select * from analytics.information_schema.table_options", "")
	options := map[string]string{}
	for _, row := range optionRows {
		if row[2] == "daily_events" {
			options[row[3]] = row[5]
		}
	}
	if options["require_partition_filter"] != "true" || options["partition_expiration_days"] != "2" {
		t.Fatalf("unexpected table options: %v", options)
	}
}

func TestRangeAndIngestionTimePartitionCounts(t *testing.T) {
	s := newTestServer()
	createPartitionedTable(t, s, "range_events", `{
  "tableReference":{"tableId":"range_events"},
  "schema":{"fields":[{"name":"bucket_id","type":"INT64"},{"name":"name","type":"STRING"}]},
  "rangePartitioning":{"field":"bucket_id","range":{"start":"0","end":"100","interval":"10"}},
  "requirePartitionFilter":true
}`)
	rangeRecord, _, _ := s.tables.get("p1", "analytics", "range_events")
	rangeRows := [][]string{{"-1", "low"}, {"0", "zero"}, {"9", "nine"}, {"10", "ten"}, {"99", "max"}, {"100", "high"}, {storedNullCell, "null"}}
	if _, err := s.tables.upsertCopyDestination(tableReference{ProjectID: "p1", DatasetID: "analytics", TableID: "range_events"}, rangeRecord.Schema, rangeRows, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append range rows: %v", err)
	}

	code, _ := syncQuery(t, s, "SELECT name FROM analytics.range_events WHERE bucket_id >= 0")
	if code != http.StatusOK {
		t.Fatalf("expected range-filtered query to succeed, got %d", code)
	}
	_, partitionRows, _ := s.simulateInformationSchemaQuery("p1", "SELECT * FROM analytics.INFORMATION_SCHEMA.PARTITIONS", "select * from analytics.information_schema.partitions", "")
	rangeCounts := map[string]string{}
	for _, row := range partitionRows {
		if row[2] == "range_events" {
			rangeCounts[row[3]] = row[4]
		}
	}
	if rangeCounts["0"] != "2" || rangeCounts["10"] != "1" || rangeCounts["90"] != "1" || rangeCounts["__UNPARTITIONED__"] != "2" || rangeCounts["__NULL__"] != "1" {
		t.Fatalf("unexpected range partition counts: %v", rangeCounts)
	}

	clock := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	s.tables.now = func() time.Time { return clock }
	createPartitionedTable(t, s, "ingestion_events", `{
  "tableReference":{"tableId":"ingestion_events"},
  "schema":{"fields":[{"name":"id","type":"INT64"}]},
  "timePartitioning":{"type":"DAY"}
}`)
	ingestionRecord, _, _ := s.tables.get("p1", "analytics", "ingestion_events")
	dest := tableReference{ProjectID: "p1", DatasetID: "analytics", TableID: "ingestion_events"}
	if _, err := s.tables.upsertCopyDestination(dest, ingestionRecord.Schema, [][]string{{"1"}}, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append first ingestion partition: %v", err)
	}
	clock = clock.Add(24 * time.Hour)
	if _, err := s.tables.upsertCopyDestination(dest, ingestionRecord.Schema, [][]string{{"2"}}, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
		t.Fatalf("append second ingestion partition: %v", err)
	}
	ingestionRecord, _, _ = s.tables.get("p1", "analytics", "ingestion_events")
	ingestionCounts := partitionCounts(ingestionRecord)
	if ingestionCounts["20260805"] != 1 || ingestionCounts["20260806"] != 1 {
		t.Fatalf("unexpected ingestion-time partitions: %v", ingestionCounts)
	}
	clock = clock.Add(24 * time.Hour)
	if _, err := s.executeQueryStatement("p1", "", "UPDATE analytics.ingestion_events SET id = 20 WHERE id = 2", "", "", nil); err != nil {
		t.Fatalf("update ingestion-time table: %v", err)
	}
	if _, err := s.executeQueryStatement("p1", "", "INSERT INTO analytics.ingestion_events (id) VALUES (3)", "", "", nil); err != nil {
		t.Fatalf("insert into ingestion-time table: %v", err)
	}
	ingestionRecord, _, _ = s.tables.get("p1", "analytics", "ingestion_events")
	ingestionCounts = partitionCounts(ingestionRecord)
	if ingestionCounts["20260805"] != 1 || ingestionCounts["20260806"] != 1 || ingestionCounts["20260807"] != 1 {
		t.Fatalf("expected UPDATE to preserve and INSERT to add ingestion partitions, got %v", ingestionCounts)
	}
}
