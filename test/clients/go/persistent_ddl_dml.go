// Command persistent_ddl_dml is the official cloud.google.com/go/bigquery
// smoke test for persistent SQL mutations and streaming inserts, mirroring
// test/clients/python/persistent_ddl_dml.py and
// test/clients/node/persistent_ddl_dml.js so all three official clients
// exercise the same real server behavior.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func runQuery(ctx context.Context, client *bigquery.Client, sql string) *bigquery.JobStatus {
	q := client.Query(sql)
	job, err := q.Run(ctx)
	if err != nil {
		log.Fatalf("run query %q: %v", sql, err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		log.Fatalf("wait for job %q: %v", sql, err)
	}
	if status.Err() != nil {
		log.Fatalf("job for %q failed: %v", sql, status.Err())
	}
	return status
}

func queryStats(status *bigquery.JobStatus) *bigquery.QueryStatistics {
	stats, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok {
		log.Fatalf("expected *bigquery.QueryStatistics, got %T", status.Statistics.Details)
	}
	return stats
}

func runDML(ctx context.Context, client *bigquery.Client, sql string, expectedAffectedRows int64) *bigquery.QueryStatistics {
	stats := queryStats(runQuery(ctx, client, sql))
	if stats.NumDMLAffectedRows != expectedAffectedRows {
		log.Fatalf("expected %d affected rows for %q, got %d", expectedAffectedRows, sql, stats.NumDMLAffectedRows)
	}
	return stats
}

type mutationRow struct {
	ID     int64               `bigquery:"id"`
	Name   bigquery.NullString `bigquery:"name"`
	Active bool                `bigquery:"active"`
}

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:19050", "LocaQL REST endpoint")
	flag.Parse()

	ctx := context.Background()
	// The client's generated REST stubs derive their full request path from
	// this endpoint directly rather than appending /bigquery/v2 themselves,
	// unlike the endpoint form the Python/Node clients expect (host only) —
	// confirmed empirically: a bare host produced 404s on LocaQL's own
	// request logs for /projects/p1/jobs (missing /bigquery/v2 entirely).
	client, err := bigquery.NewClient(ctx, "p1",
		option.WithEndpoint(*endpoint+"/bigquery/v2/"),
		option.WithoutAuthentication(),
	)
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer client.Close()

	runQuery(ctx, client, "CREATE OR REPLACE TABLE analytics.go_sql_mutations (id INT64, name STRING, active BOOL)")

	inserted := runDML(ctx, client,
		"INSERT INTO analytics.go_sql_mutations (id, name, active) VALUES (1, 'one', TRUE), (2, NULL, FALSE)", 2)
	updated := runDML(ctx, client,
		"UPDATE analytics.go_sql_mutations SET name = 'two', active = TRUE WHERE id = 2", 1)

	runQuery(ctx, client,
		"CREATE OR REPLACE TABLE analytics.go_sql_merge_source AS "+
			"SELECT 2 AS id, 'two-merged' AS name, FALSE AS active UNION ALL SELECT 3, 'three', TRUE")
	merged := runDML(ctx, client,
		"MERGE INTO analytics.go_sql_mutations AS T "+
			"USING analytics.go_sql_merge_source AS S ON T.id = S.id "+
			"WHEN MATCHED THEN UPDATE SET name = S.name, active = S.active "+
			"WHEN NOT MATCHED THEN INSERT (id, name, active) VALUES (S.id, S.name, S.active)", 2)
	deleted := runDML(ctx, client, "DELETE FROM analytics.go_sql_mutations WHERE id = 1", 1)

	// Streaming insert (tabledata.insertAll) via the official Inserter, not a
	// query job — real BigQuery Go client code uses this for row ingestion.
	inserter := client.Dataset("analytics").Table("go_sql_mutations").Inserter()
	if err := inserter.Put(ctx, []*mutationRow{{ID: 4, Name: bigquery.NullString{StringVal: "four", Valid: true}, Active: true}}); err != nil {
		log.Fatalf("streaming insert: %v", err)
	}

	it, err := client.Query("SELECT id, name, active FROM analytics.go_sql_mutations ORDER BY id").Read(ctx)
	if err != nil {
		log.Fatalf("read rows: %v", err)
	}
	type row struct {
		ID     int64
		Name   string
		Active bool
	}
	var got []row
	for {
		var r row
		err := it.Next(&r)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("iterate rows: %v", err)
		}
		got = append(got, r)
	}
	want := []row{
		{ID: 2, Name: "two-merged", Active: false},
		{ID: 3, Name: "three", Active: true},
		{ID: 4, Name: "four", Active: true},
	}
	if len(got) != len(want) {
		log.Fatalf("persistent DML + streaming insert rows mismatch: got %+v, want %+v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			log.Fatalf("persistent DML + streaming insert rows mismatch at %d: got %+v, want %+v", i, got[i], want[i])
		}
	}

	runQuery(ctx, client, "DROP TABLE analytics.go_sql_merge_source")

	rowsOut := make([]map[string]any, len(got))
	for i, r := range got {
		rowsOut[i] = map[string]any{"id": r.ID, "name": r.Name, "active": r.Active}
	}
	out, err := json.Marshal(map[string]any{
		"delete": deleted.NumDMLAffectedRows,
		"insert": inserted.NumDMLAffectedRows,
		"merge":  merged.NumDMLAffectedRows,
		"rows":   rowsOut,
		"update": updated.NumDMLAffectedRows,
	})
	if err != nil {
		log.Fatalf("marshal report: %v", err)
	}
	fmt.Println(string(out))
}
