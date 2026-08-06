package server

import (
	"context"
	"net"
	"testing"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// newTestStorageGRPCConn boots a real grpc.Server backed by s (over an
// in-memory bufconn listener, no real socket needed) with both the Read and
// Write services registered (NewStorageGRPCServer registers both), and
// returns a real *grpc.ClientConn talking to it — this exercises the actual
// wire protocol (protobuf/grpc framing, not just direct Go method calls),
// the same fidelity bar httptest.NewRecorder gives the REST handlers
// elsewhere in this test suite.
func newTestStorageGRPCConn(t *testing.T, s *Server) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	gs := s.NewStorageGRPCServer()
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newTestStorageReadClient(t *testing.T, s *Server) storagepb.BigQueryReadClient {
	t.Helper()
	return storagepb.NewBigQueryReadClient(newTestStorageGRPCConn(t, s))
}

func readAllStorageRows(t *testing.T, resp *storagepb.ReadRowsResponse) []map[string]any {
	t.Helper()
	avroRows := resp.GetAvroRows()
	if avroRows == nil {
		t.Fatalf("expected AvroRows in response, got %v", resp.GetRows())
	}
	schemaJSON := resp.GetAvroSchema().GetSchema()
	if schemaJSON == "" {
		t.Fatalf("expected an avro schema on the first ReadRowsResponse")
	}
	codec, err := goavro.NewCodec(schemaJSON)
	if err != nil {
		t.Fatalf("build avro codec from returned schema: %v", err)
	}

	var rows []map[string]any
	buf := avroRows.GetSerializedBinaryRows()
	for len(buf) > 0 {
		native, rest, err := codec.NativeFromBinary(buf)
		if err != nil {
			t.Fatalf("decode avro row: %v", err)
		}
		row := native.(map[string]any)
		for name, value := range row {
			row[name] = unwrapAvroUnion(value)
		}
		rows = append(rows, row)
		buf = rest
	}
	if int64(len(rows)) != resp.GetRowCount() {
		t.Fatalf("decoded %d rows but RowCount said %d", len(rows), resp.GetRowCount())
	}
	return rows
}

func storageTableName(datasetID, tableID string) string {
	return "projects/p1/datasets/" + datasetID + "/tables/" + tableID
}

func idNameFields() []map[string]any {
	return []map[string]any{
		{"name": "id", "type": "INT64"},
		{"name": "name", "type": "STRING"},
	}
}

func TestStorageReadSessionAndReadRowsRoundTrip(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "storage_events", idNameFields(),
		"id,name\n1,alpha\n2,beta\n3,gamma\n")

	client := newTestStorageReadClient(t, s)
	ctx := context.Background()

	session, err := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent:      "projects/p1",
		ReadSession: &storagepb.ReadSession{Table: storageTableName("analytics", "storage_events")},
	})
	if err != nil {
		t.Fatalf("CreateReadSession: %v", err)
	}
	if session.GetDataFormat() != storagepb.DataFormat_AVRO {
		t.Fatalf("expected DATA_FORMAT_AVRO, got %v", session.GetDataFormat())
	}
	if len(session.GetStreams()) != 1 {
		t.Fatalf("expected exactly 1 stream (bounded scope), got %d", len(session.GetStreams()))
	}
	if session.GetEstimatedRowCount() != 3 {
		t.Fatalf("expected EstimatedRowCount=3, got %d", session.GetEstimatedRowCount())
	}

	rowsClient, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: session.GetStreams()[0].GetName()})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	resp, err := rowsClient.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	rows := readAllStorageRows(t, resp)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	names := map[string]bool{}
	for _, r := range rows {
		names[r["name"].(string)] = true
	}
	for _, want := range []string{"alpha", "beta", "gamma"} {
		if !names[want] {
			t.Errorf("expected row %q in the decoded avro rows, got %v", want, rows)
		}
	}
}

func TestStorageReadSessionAppliesColumnProjection(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "storage_projection", idNameFields(),
		"id,name\n1,alpha\n2,beta\n")

	client := newTestStorageReadClient(t, s)
	ctx := context.Background()

	session, err := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: "projects/p1",
		ReadSession: &storagepb.ReadSession{
			Table:       storageTableName("analytics", "storage_projection"),
			ReadOptions: &storagepb.ReadSession_TableReadOptions{SelectedFields: []string{"name"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateReadSession: %v", err)
	}

	rowsClient, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: session.GetStreams()[0].GetName()})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	resp, err := rowsClient.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	rows := readAllStorageRows(t, resp)
	for _, r := range rows {
		if _, hasID := r["id"]; hasID {
			t.Fatalf("expected only the projected 'name' column, got id in row %v", r)
		}
		if _, hasName := r["name"]; !hasName {
			t.Fatalf("expected the projected 'name' column, got %v", r)
		}
	}
}

func TestStorageReadSessionAppliesRowRestriction(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "storage_restriction", idNameFields(),
		"id,name\n1,alpha\n2,beta\n3,gamma\n")

	client := newTestStorageReadClient(t, s)
	ctx := context.Background()

	session, err := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: "projects/p1",
		ReadSession: &storagepb.ReadSession{
			Table:       storageTableName("analytics", "storage_restriction"),
			ReadOptions: &storagepb.ReadSession_TableReadOptions{RowRestriction: "id > 1"},
		},
	})
	if err != nil {
		t.Fatalf("CreateReadSession: %v", err)
	}
	if session.GetEstimatedRowCount() != 2 {
		t.Fatalf("expected row_restriction to leave 2 rows, got %d", session.GetEstimatedRowCount())
	}

	rowsClient, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: session.GetStreams()[0].GetName()})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	resp, err := rowsClient.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	rows := readAllStorageRows(t, resp)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after row_restriction, got %d", len(rows))
	}
	for _, r := range rows {
		if r["name"] == "alpha" {
			t.Fatalf("expected row_restriction 'id > 1' to exclude id=1 (alpha), got %v", rows)
		}
	}
}

func TestStorageReadSessionTableNotFoundReturnsNotFound(t *testing.T) {
	s := newTestServer()
	client := newTestStorageReadClient(t, s)

	_, err := client.CreateReadSession(context.Background(), &storagepb.CreateReadSessionRequest{
		Parent:      "projects/p1",
		ReadSession: &storagepb.ReadSession{Table: storageTableName("analytics", "does_not_exist")},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestStorageReadSessionRejectsArrowFormat(t *testing.T) {
	s := newTestServer()
	loadCSVTable(t, s, "analytics", "storage_arrow", idNameFields(), "id,name\n1,alpha\n")

	client := newTestStorageReadClient(t, s)
	_, err := client.CreateReadSession(context.Background(), &storagepb.CreateReadSessionRequest{
		Parent: "projects/p1",
		ReadSession: &storagepb.ReadSession{
			Table: storageTableName("analytics", "storage_arrow"),
			ReadOptions: &storagepb.ReadSession_TableReadOptions{
				OutputFormatSerializationOptions: &storagepb.ReadSession_TableReadOptions_ArrowSerializationOptions{
					ArrowSerializationOptions: &storagepb.ArrowSerializationOptions{},
				},
			},
		},
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented for Arrow framing, got %v", err)
	}
}

func TestStorageReadRowsUnknownStreamReturnsNotFound(t *testing.T) {
	s := newTestServer()
	client := newTestStorageReadClient(t, s)

	rowsClient, err := client.ReadRows(context.Background(), &storagepb.ReadRowsRequest{ReadStream: "projects/p1/locations/local/sessions/nope/streams/nope"})
	if err != nil {
		t.Fatalf("ReadRows call itself should not fail: %v", err)
	}
	_, err = rowsClient.Recv()
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound for an unknown stream, got %v", err)
	}
}

func TestStorageSplitReadStreamIsUnimplemented(t *testing.T) {
	s := newTestServer()
	client := newTestStorageReadClient(t, s)

	_, err := client.SplitReadStream(context.Background(), &storagepb.SplitReadStreamRequest{Name: "projects/p1/locations/local/sessions/x/streams/y"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented, got %v", err)
	}
}

func TestStorageReadSessionRejectsNestedSchema(t *testing.T) {
	s := newTestServer()
	loadNDJSONTable(t, s, "analytics", "storage_nested",
		[]map[string]any{
			{"name": "id", "type": "INT64"},
			{"name": "info", "type": "RECORD", "fields": []map[string]any{
				{"name": "city", "type": "STRING"},
			}},
		},
		`{"id":1,"info":{"city":"Bogota"}}`+"\n")

	client := newTestStorageReadClient(t, s)
	_, err := client.CreateReadSession(context.Background(), &storagepb.CreateReadSessionRequest{
		Parent:      "projects/p1",
		ReadSession: &storagepb.ReadSession{Table: storageTableName("analytics", "storage_nested")},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for a nested schema (Avro-only scope), got %v", err)
	}
}
