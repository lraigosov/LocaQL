// Command storage_arrow is the official cloud.google.com/go/bigquery smoke
// test for DATA_FORMAT_ARROW Storage Read: Table.Read, once
// EnableStorageReadClient points the client at LocaQL's Storage API gRPC
// listener, always requests DATA_FORMAT_ARROW internally (see
// cloud.google.com/go/bigquery's storage_client.go) — this test exercises
// that real, hardcoded-Arrow code path end to end, not a hand-rolled
// decoder, mirroring persistent_ddl_dml.go's use of the same official
// client for REST.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sort"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type row struct {
	ID     int64               `bigquery:"id"`
	Name   bigquery.NullString `bigquery:"name"`
	Active bool                `bigquery:"active"`
}

func main() {
	restEndpoint := flag.String("endpoint", "http://127.0.0.1:19050", "LocaQL REST endpoint")
	storageEndpoint := flag.String("storage-endpoint", "127.0.0.1:19060", "LocaQL Storage API gRPC endpoint (host:port, no scheme)")
	flag.Parse()

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "p1",
		option.WithEndpoint(*restEndpoint+"/bigquery/v2/"),
		option.WithoutAuthentication(),
	)
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer client.Close()

	// EnableStorageReadClient dials the Storage API gRPC listener separately
	// from the REST client above — a real plaintext connection (no TLS) to
	// LocaQL's own --storage-grpc-addr, the same anonymous-only convention
	// every other client in this project uses.
	if err := client.EnableStorageReadClient(ctx,
		option.WithEndpoint(*storageEndpoint),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	); err != nil {
		log.Fatalf("enable storage read client: %v", err)
	}

	mustRun(ctx, client, "CREATE OR REPLACE TABLE analytics.go_storage_arrow (id INT64, name STRING, active BOOL)")
	mustRun(ctx, client, "INSERT INTO analytics.go_storage_arrow (id, name, active) VALUES "+
		"(1, 'one', TRUE), (2, NULL, FALSE), (3, 'three', TRUE)")

	// Table.Read takes the Storage API path once EnableStorageReadClient has
	// succeeded (see (*Table).read -> c.isStorageReadAvailable() in the
	// official client's table.go), and that path always requests
	// DataFormat_ARROW unconditionally (storage_client.go's
	// createReadSessionRequest, verified against the pinned client source
	// before writing this test) — so a successful read here has necessarily
	// gone through this project's Arrow implementation, not just proto/REST.
	// ArrowIterator() confirms that path was actually reached (a non-empty
	// serialized schema, real bytes this project's Arrow schema builder
	// produced) before decoding full application-level rows through the
	// same client's normal, most-used entry point (RowIterator.Next).
	table := client.Dataset("analytics").Table("go_storage_arrow")
	it := table.Read(ctx)
	arrowIt, err := it.ArrowIterator()
	if err != nil {
		log.Fatalf("expected an ArrowIterator (Storage API + Arrow path), got error: %v", err)
	}
	if len(arrowIt.SerializedArrowSchema()) == 0 {
		log.Fatalf("expected a non-empty serialized Arrow schema from the real Storage Read session")
	}

	it = table.Read(ctx)
	var got []row
	for {
		var r row
		err := it.Next(&r)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("iterate rows via Storage API (Arrow): %v", err)
		}
		got = append(got, r)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].ID < got[j].ID })

	want := []row{
		{ID: 1, Name: bigquery.NullString{StringVal: "one", Valid: true}, Active: true},
		{ID: 2, Name: bigquery.NullString{}, Active: false},
		{ID: 3, Name: bigquery.NullString{StringVal: "three", Valid: true}, Active: true},
	}
	if len(got) != len(want) {
		log.Fatalf("expected %d rows via Storage API Arrow read, got %d: %+v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			log.Fatalf("row %d mismatch: got %+v, want %+v", i, got[i], want[i])
		}
	}

	out, err := json.Marshal(map[string]any{"rows": len(got), "arrowSchemaBytes": len(arrowIt.SerializedArrowSchema())})
	if err != nil {
		log.Fatalf("marshal summary: %v", err)
	}
	fmt.Println(string(out))
}

func mustRun(ctx context.Context, client *bigquery.Client, sql string) {
	job, err := client.Query(sql).Run(ctx)
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
}
